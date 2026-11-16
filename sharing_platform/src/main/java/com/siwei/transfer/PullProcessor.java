package com.siwei.transfer;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.siwei.endpoint.*;
import com.siwei.protocol.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.time.Instant;
import java.util.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
@ConditionalOnProperty(name = "platform.pulls.enabled", havingValue = "true", matchIfMissing = true)
public class PullProcessor implements TaskProcessor {
    private final PullStore db;
    private final TransferJpa outbox;
    private final ObjectMapper json;
    private final EndpointRevisionRepository revisions;
    private final RemoteFileClientFactory clients;
    private final TransferSettings settings;
    private final TransactionTemplate tx;

    public PullProcessor(PullStore db, TransferJpa outbox, ObjectMapper json, EndpointRevisionRepository revisions,
                         RemoteFileClientFactory clients, TransferSettings settings,
                         org.springframework.transaction.PlatformTransactionManager manager) {
        this.db = db; this.outbox = outbox; this.json = json; this.revisions = revisions;
        this.clients = clients; this.settings = settings; this.tx = new TransactionTemplate(manager);
    }
    @Override public String direction() { return "PULL"; }

    @Override public Result process(Task task) throws Exception {
        var record = db.one("SELECT pull_config,pull_scan_complete,claim_id FROM transfer_task WHERE id=?", task.taskRecordId);
        JsonNode config = json.readTree((String) record.get("pull_config"));
        EndpointRevision endpoint = revisions.findById(task.endpointRevisionId).orElseThrow();
        String scope = ControlledPaths.resolve(endpoint.readRoot, json.readTree(task.canonicalRequest).path("sourcePath").asText());
        String claim = (String) record.get("claim_id");
        if (claim == null || !claim.equals(task.claimId)) throw new IllegalStateException("TASK_NOT_CLAIMED");
        List<Map<String, Object>> units = db.rows("SELECT * FROM pull_task_unit WHERE task_id=? ORDER BY source_path", task.taskRecordId);
        boolean needsRemote = !(Boolean) record.get("pull_scan_complete") || units.stream().anyMatch(u -> u.get("transfer_id") == null && !"FAILED".equals(u.get("status")));
        boolean workNeeded = needsRemote;
        for (var unit : units) if (unit.get("transfer_id") != null) {
            String state = (String) db.one("SELECT status FROM pull_transfer WHERE id=?", unit.get("transfer_id")).get("status");
            if (!Set.of("PUBLISHED", "FAILED").contains(state)) {
                workNeeded = true;
                if (!state.equals("PUBLISHING")) needsRemote = true;
            }
        }
        if (workNeeded) try (RemoteFileClient remote = needsRemote ? clients.connect(endpoint) : null) {
            if (!(Boolean) record.get("pull_scan_complete")) {
                List<String> paths = discover(remote, scope, config, task);
                if (paths == null) return waiting();
                tx.executeWithoutResult(status -> {
                    for (String path : paths) db.update("INSERT INTO pull_task_unit(task_id,source_path) VALUES (?,?) ON CONFLICT DO NOTHING", task.taskRecordId, path);
                    db.update("UPDATE transfer_task SET pull_scan_complete=true,attempts=0 WHERE id=? AND claim_id=?", task.taskRecordId, claim);
                });
                units = db.rows("SELECT * FROM pull_task_unit WHERE task_id=? ORDER BY source_path", task.taskRecordId);
            }
            for (var unit : units) {
                String path = (String) unit.get("source_path");
                if ("FAILED".equals(unit.get("status"))) continue;
                String transfer = (String) unit.get("transfer_id");
                if (transfer != null) {
                    var delivery = transfer(transfer);
                    if ("SOURCE_CHANGED".equals(delivery.get("status"))) {
                        db.update("UPDATE pull_task_unit SET transfer_id=NULL,status='WAITING_SOURCE' WHERE task_id=? AND source_path=?", task.taskRecordId, path);
                        transfer = null;
                    }
                }
                if (transfer == null) {
                    candidate(task, path, config);
                    try {
                        ObjectNode observation = observe(remote, path, config);
                        transfer = freeze(task, path, config, observation);
                        db.update("UPDATE pull_task_unit SET attempts=0 WHERE task_id=? AND source_path=?", task.taskRecordId, path);
                    } catch (NotReady error) {
                        db.update("UPDATE pull_candidate SET last_error=? WHERE candidate_key=?", error.getMessage(), candidateKey(task, path, config));
                        db.update("UPDATE pull_task_unit SET error_type=? WHERE task_id=? AND source_path=?", error.getMessage(), task.taskRecordId, path);
                        continue;
                    } catch (IOException | IllegalArgumentException error) {
                        unitFailure(task, path, error);
                        continue;
                    }
                }
                if (transfer != null) execute(remote, task, transfer, claim, endpoint.concurrency);
            }
        }
        remind(task.taskRecordId, config.path("acceptanceSeconds").asInt());
        // The worker recomputes persisted unit states under a lock before releasing its claim.
        return waiting();
    }

    public void finish(String task, String claim) {
        String config = (String) db.one("SELECT pull_config FROM transfer_task WHERE id=?", task).get("pull_config");
        try { db.aggregate(task, claim, json.readTree(config).path("observationSeconds").asInt()); }
        catch (com.fasterxml.jackson.core.JsonProcessingException e) { throw new IllegalStateException(e); }
    }
    private static Result waiting() { return new Result("WAITING_SOURCE", "PULL", null, null); }

    private List<String> discover(RemoteFileClient remote, String scope, JsonNode config, Task task) throws IOException {
        if (!exists(remote, scope, config.path("remoteRoot").asText())) return null;
        if (config.path("scanDepth").asInt() == 0) return List.of(scope);
        List<String> result = new ArrayList<>();
        discoverAt(remote, scope, config.path("scanDepth").asInt(), config, result, task);
        Collections.sort(result);
        return result;
    }
    private void discoverAt(RemoteFileClient remote, String root, int depth, JsonNode config, List<String> found, Task task) throws IOException {
        for (RemoteFileClient.Entry entry : remote.list(root)) {
            safeName(entry.name); ordinary(entry);
            String path = root + "/" + entry.name;
            if (depth > 1) {
                if (entry.directory) discoverAt(remote, path, depth - 1, config, found, task);
            } else if ((config.path("unitType").asText().equals("FILE") ? entry.regular : entry.directory)
                    && !isControl(entry.name, config) && matches(entry.name, config)) {
                candidate(task, path, config); found.add(path);
            }
            if (found.size() > 10000) throw new InvalidSource("TOO_MANY_UNITS");
        }
    }
    private static boolean matches(String name, JsonNode config) {
        return FileSystems.getDefault().getPathMatcher("glob:" + config.path("filePattern").asText()).matches(Paths.get(name));
    }

    ObjectNode observe(RemoteFileClient remote, String path, JsonNode config) throws IOException {
        if (!exists(remote, path, config.path("remoteRoot").asText())) throw new NotReady("SOURCE_MISSING");
        boolean file = config.path("unitType").asText().equals("FILE");
        RemoteFileClient.Entry root = remote.lstat(path); ordinary(root);
        if (file != root.regular) throw new InvalidSource("UNIT_TYPE_MISMATCH");
        ObjectNode observed = json.createObjectNode();
        ArrayNode files = observed.putArray("files"), dirs = observed.putArray("directories");
        if (file) addFile(files, path.substring(path.lastIndexOf('/') + 1), root);
        else scanTree(remote, path, "", config, files, dirs, 0);
        List<JsonNode> ordered = new ArrayList<>(); files.forEach(ordered::add);
        ordered.sort(Comparator.comparing(n -> n.path("path").asText())); files.removeAll(); ordered.forEach(files::add);
        List<String> directories = new ArrayList<>(); dirs.forEach(n -> directories.add(n.asText()));
        Collections.sort(directories); dirs.removeAll(); directories.forEach(dirs::add);
        Set<String> folded = new HashSet<>();
        for (JsonNode f : files) if (!folded.add(f.path("path").asText().toLowerCase(Locale.ROOT))) throw new InvalidSource("CASE_COLLISION");
        for (JsonNode d : dirs) if (!folded.add(d.asText().toLowerCase(Locale.ROOT))) throw new InvalidSource("CASE_COLLISION");
        String manifestName = config.path("manifestName").asText(null), readyName = config.path("readyName").asText(null);
        String manifestSha = null;
        int controlFiles = 0; long controlBytes = 0;
        JsonNode expected = null;
        if (manifestName != null && exists(remote, control(path, file, manifestName), config.path("remoteRoot").asText())) {
            byte[] bytes = controlBytes(remote, control(path, file, manifestName));
            controlFiles++; controlBytes += bytes.length;
            manifestSha = hex(sha().digest(bytes));
            JsonNode manifest = json.readTree(bytes);
            if (!manifest.isObject() || !manifest.path("version").isTextual() || manifest.path("version").asText().isBlank()
                    || !manifest.path("files").isObject()) throw new InvalidSource("INVALID_MANIFEST");
            expected = manifest.path("files");
            observed.put("manifestSha256", manifestSha); observed.put("upstreamVersion", manifest.path("version").asText());
            if (expected.size() != files.size()) throw new NotReady("MANIFEST_SET_MISMATCH");
            for (JsonNode f : files) {
                JsonNode hash = expected.get(f.path("path").asText());
                if (hash == null || !hash.isTextual() || !hash.asText().matches("[a-fA-F0-9]{64}")) throw new InvalidSource("INVALID_EXPECTED_HASH");
                ((ObjectNode) f).put("expectedHash", hash.asText().toLowerCase(Locale.ROOT));
            }
        }
        boolean ready = true;
        String reason = null;
        if (files.isEmpty()) { ready = false; reason = "EMPTY_UNIT"; }
        for (JsonNode f : files) if (f.path("notReady").asBoolean()) { ready = false; reason = "EMPTY_OR_TEMPORARY_FILE"; }
        if (config.path("validation").asText().equals("HASH") && expected == null) { ready = false; reason = "EXPECTED_HASH_REQUIRED"; }
        if (manifestName != null && manifestSha == null) { ready = false; reason = "MANIFEST_MISSING"; }
        if (manifestSha == null) for (JsonNode f : files) if (f.path("modifiedMillis").isNull()) {
            ready = false; reason = "SOURCE_TIME_UNAVAILABLE";
        }
        if (readyName != null) {
            String marker = control(path, file, readyName);
            if (!exists(remote, marker, config.path("remoteRoot").asText())) { ready = false; reason = "READY_MISSING"; }
            else {
                byte[] bytes = controlBytes(remote, marker);
                controlFiles++; controlBytes += bytes.length;
                String text = new String(bytes, StandardCharsets.UTF_8).trim();
                observed.put("readySha256", hex(sha().digest(bytes)));
                if (manifestSha != null && !manifestSha.equals(text)) { ready = false; reason = "READY_VERSION_MISMATCH"; }
            }
        }
        observed.put("ready", ready);
        observed.put("controlFiles", controlFiles); observed.put("controlBytes", controlBytes);
        if (reason != null) observed.put("reason", reason);
        return observed;
    }
    private void scanTree(RemoteFileClient remote, String root, String relative, JsonNode config,
                          ArrayNode files, ArrayNode dirs, int depth) throws IOException {
        if (depth > 32) throw new InvalidSource("SOURCE_TOO_DEEP");
        List<RemoteFileClient.Entry> entries = remote.list(root);
        entries.sort(Comparator.comparing(e -> e.name));
        for (RemoteFileClient.Entry entry : entries) {
            safeName(entry.name); ordinary(entry);
            if (relative.isEmpty() && isControl(entry.name, config)) continue;
            String name = relative.isEmpty() ? entry.name : relative + "/" + entry.name;
            if (entry.directory) {
                dirs.add(name); scanTree(remote, root + "/" + entry.name, name, config, files, dirs, depth + 1);
            } else addFile(files, name, remote.lstat(root + "/" + entry.name));
            if (files.size() + dirs.size() > 100000) throw new InvalidSource("SOURCE_TOO_LARGE");
        }
    }
    private void addFile(ArrayNode files, String name, RemoteFileClient.Entry entry) throws IOException {
        safeRelative(name); ordinary(entry);
        if (!entry.regular || entry.size < 0) throw new InvalidSource("NOT_REGULAR_FILE");
        ObjectNode f = files.addObject(); f.put("path", name); f.put("size", entry.size);
        if ((!settings.isAllowZeroByteFiles() && entry.size == 0)
                || settings.getTemporarySuffixes().stream().anyMatch(s -> s != null && !s.isEmpty() && name.endsWith(s))) f.put("notReady", true);
        if (entry.modifiedMillis == null) f.putNull("modifiedMillis"); else f.put("modifiedMillis", entry.modifiedMillis);
    }

    private String freeze(Task task, String path, JsonNode config, ObjectNode observation) {
        String candidate = candidateKey(task, path, config);
        String observed = observation.toString();
        return tx.execute(status -> {
            db.update("INSERT INTO pull_candidate(candidate_key,source_id,source_path,observation,stable_count,observed_at) VALUES (?,?,?,?,1,now()) ON CONFLICT DO NOTHING",
                    candidate, config.path("sourceId").asText(), path, observed);
            Map<String, Object> row = db.one("SELECT *,observed_at<=now()-(? * interval '1 second') AS due FROM pull_candidate WHERE candidate_key=? FOR UPDATE",
                    config.path("observationSeconds").asInt(), candidate);
            int stable = ((Number) row.get("stable_count")).intValue();
            db.update("UPDATE pull_candidate SET last_error=NULL WHERE candidate_key=?", candidate);
            if (!observed.equals(row.get("observation"))) {
                stable = 1;
                db.update("UPDATE pull_candidate SET observation=?,stable_count=1,observed_at=now(),last_error=NULL WHERE candidate_key=?", observed, candidate);
            } else if ((Boolean) row.get("due")) {
                stable++;
                db.update("UPDATE pull_candidate SET stable_count=?,observed_at=now(),last_error=NULL WHERE candidate_key=?", stable, candidate);
            }
            String reason = !observation.path("ready").asBoolean() ? observation.path("reason").asText() : null;
            if (reason == null && !config.hasNonNull("readyName") && stable < config.path("stableObservations").asInt()) reason = "STABILIZING";
            if (reason != null) {
                db.update("UPDATE pull_task_unit SET error_type=? WHERE task_id=? AND source_path=?", reason, task.taskRecordId, path);
                return null;
            }
            String basis = observation.has("manifestSha256") ? "MANIFEST_SHA256" : "SIZE_MTIME_SET";
            String version = TaskWorker.sha256(basis + "\n" + (observation.has("manifestSha256") ? observation.path("manifestSha256").asText() : observed));
            String key = TaskWorker.sha256(task.endpointId + "\n" + path + "\n" + rule(config) + "\n" + basis + "\n" + version + "\n" + deliveryScope(config));
            db.update("INSERT INTO pull_delivery_key(delivery_key) VALUES (?) ON CONFLICT DO NOTHING", key);
            String current = (String) db.one("SELECT current_transfer_id FROM pull_delivery_key WHERE delivery_key=? FOR UPDATE", key).get("current_transfer_id");
            if (current == null || "SOURCE_CHANGED".equals(transfer(current).get("status")))
                current = create(task.taskRecordId, task.endpointId, path, version, basis, observed, config.toString(), key, current);
            link(task.taskRecordId, path, current);
            return current;
        });
    }
    private String create(String task, String endpoint, String path, String version, String basis, String observed, String config, String key, String previous) {
        String id = UUID.randomUUID().toString();
        db.update("INSERT INTO pull_transfer(id,delivery_key,previous_transfer_id,owner_task_id,endpoint_id,source_path,source_version,version_basis,version_strength,observation,config) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                id, key, previous, task, endpoint, path, version, basis, basis.equals("MANIFEST_SHA256") ? "STRONG" : "WEAK", observed, config);
        db.update("UPDATE pull_delivery_key SET current_transfer_id=? WHERE delivery_key=?", id, key);
        try {
            for (JsonNode f : json.readTree(observed).path("files")) db.update("INSERT INTO pull_file(transfer_id,relative_path,size_bytes,expected_hash) VALUES (?,?,?,?)",
                    id, f.path("path").asText(), f.path("size").asLong(), f.path("expectedHash").asText(null));
        } catch (com.fasterxml.jackson.core.JsonProcessingException error) { throw new IllegalStateException(error); }
        return id;
    }
    private void link(String task, String path, String transfer) {
        db.update("INSERT INTO pull_task_transfer(task_id,transfer_id) VALUES (?,?) ON CONFLICT DO NOTHING", task, transfer);
        db.update("UPDATE pull_task_unit SET transfer_id=?,status='LINKED',error_type=NULL WHERE task_id=? AND source_path=?", transfer, task, path);
    }
    private static String rule(JsonNode c) { return c.path("unitType").asText() + "\n" + c.path("filePattern").asText() + "\n" + c.path("manifestName").asText() + "\n" + c.path("readyName").asText(); }
    private static String candidateKey(Task task, String path, JsonNode config) {
        return TaskWorker.sha256(config.path("sourceId").asText() + "\n" + task.endpointId + "\n" + path + "\n" + rule(config)
                + "\n" + config.path("stableObservations").asInt() + "\n" + config.path("observationSeconds").asInt());
    }
    private void candidate(Task task, String path, JsonNode config) {
        db.update("INSERT INTO pull_candidate(candidate_key,source_id,source_path,observation,stable_count,observed_at,last_error) VALUES (?,?,?,'{}',0,now(),'NOT_OBSERVED') ON CONFLICT DO NOTHING",
                candidateKey(task, path, config), config.path("sourceId").asText(), path);
    }
    private static String deliveryScope(JsonNode c) {
        return c.path("localRoot").asText() + "\n" + c.path("accessBase").asText() + "\n" + c.path("recipientId").asText() + "\n" + c.path("validation").asText();
    }

    private void execute(RemoteFileClient remote, Task task, String id, String claim, int concurrency) throws Exception {
        boolean acquired = tx.execute(status -> {
            db.one("SELECT id FROM endpoint WHERE id=? FOR UPDATE", task.endpointId);
            long active = ((Number) db.one("SELECT count(*) AS n FROM pull_transfer WHERE endpoint_id=? AND claim_id IS NOT NULL AND claimed_at>now()-interval '5 minutes'", task.endpointId).get("n")).longValue();
            if (active >= concurrency) return false;
            return db.update("UPDATE pull_transfer SET claim_id=?,claimed_at=now(),attempts=attempts+1 WHERE id=? AND status IN ('DOWNLOADING','PUBLISHING') AND next_run_at<=now() AND (claim_id IS NULL OR claimed_at<now()-interval '5 minutes')", claim, id) == 1;
        });
        if (!acquired) return;
        try {
            Map<String, Object> transfer = transfer(id);
            JsonNode config = json.readTree((String) transfer.get("config"));
            JsonNode observation = json.readTree((String) transfer.get("observation"));
            Path root = ControlledPaths.localDirectory(config.path("localRoot").asText());
            Path temporary = directory(directory(root, "tmp"), id);
            Path pending = directory(root, "pending").resolve(id);
            if (!(Boolean) transfer.get("publish_intent")) {
                if (!observe(remote, (String) transfer.get("source_path"), config).toString().equals(observation.toString())) throw new Changed();
                download(remote, transfer, config, observation, temporary, claim);
                if (!observe(remote, (String) transfer.get("source_path"), config).toString().equals(observation.toString())) throw new Changed();
                if (db.update("UPDATE pull_transfer SET publish_intent=true,status='PUBLISHING' WHERE id=? AND claim_id=?", id, claim) != 1)
                    throw new IllegalStateException("DELIVERY_CLAIM_LOST");
            }
            publish(id, temporary, pending, observation);
            tx.executeWithoutResult(status -> {
                if (db.update("UPDATE pull_transfer SET status='PUBLISHED',acceptance_status='PENDING',published_at=now(),data_location=?,claim_id=NULL,claimed_at=NULL,error_type=NULL WHERE id=? AND claim_id=?",
                        location(config, id), id, claim) != 1) throw new IllegalStateException("DELIVERY_CLAIM_LOST");
                notification(id, true, null);
            });
        } catch (Exception error) {
            Map<String, Object> current = transfer(id);
            if ((Boolean) current.get("publish_intent")) {
                db.update("UPDATE pull_transfer SET claim_id=NULL,claimed_at=NULL,error_type='PUBLICATION_UNKNOWN',next_run_at=now()+interval '30 seconds' WHERE id=? AND claim_id=?", id, claim);
            } else if (error instanceof Changed || error instanceof NotReady) {
                tx.executeWithoutResult(status -> {
                    if (db.update("UPDATE pull_transfer SET status='SOURCE_CHANGED',error_type='SOURCE_CHANGED',claim_id=NULL,claimed_at=NULL WHERE id=? AND claim_id=?", id, claim) != 1) return;
                    db.update("UPDATE pull_candidate SET stable_count=0,observed_at=now(),last_error='SOURCE_CHANGED' WHERE source_path=? AND source_id=(SELECT cast(config AS json)->>'sourceId' FROM pull_transfer WHERE id=?)", current.get("source_path"), id);
                    notification(id, false, "SOURCE_CHANGED");
                    db.update("UPDATE pull_task_unit SET transfer_id=NULL,status='WAITING_SOURCE',error_type='SOURCE_CHANGED' WHERE transfer_id=?", id);
                });
            } else if (error instanceof InvalidSource || error instanceof IllegalArgumentException || !(error instanceof IOException)
                    || ((Number) current.get("attempts")).intValue() >= 4) {
                tx.executeWithoutResult(status -> {
                    if (db.update("UPDATE pull_transfer SET status='FAILED',error_type=?,claim_id=NULL,claimed_at=NULL WHERE id=? AND claim_id=?", reason(error), id, claim) != 1) return;
                    notification(id, false, reason(error));
                });
            } else db.update("UPDATE pull_transfer SET error_type=?,claim_id=NULL,claimed_at=NULL,next_run_at=now()+interval '5 seconds' WHERE id=? AND claim_id=?", reason(error), id, claim);
        }
    }
    private void download(RemoteFileClient remote, Map<String, Object> transfer, JsonNode config, JsonNode observation, Path temporary, String claim) throws IOException {
        String id = (String) transfer.get("id"), source = (String) transfer.get("source_path");
        for (JsonNode dir : observation.path("directories")) local(temporary, dir.asText(), true);
        for (JsonNode f : observation.path("files")) {
            Path path = local(temporary, f.path("path").asText(), false);
            if (Files.exists(path, LinkOption.NOFOLLOW_LINKS) && !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) throw new InvalidSource("LOCAL_SPECIAL_FILE");
            var previous = db.one("SELECT verified,actual_hash FROM pull_file WHERE transfer_id=? AND relative_path=?", id, f.path("path").asText());
            if ((Boolean) previous.get("verified") && Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                    && Files.size(path) == f.path("size").asLong() && digest(path).equals(previous.get("actual_hash"))) continue;
            db.fileProgress(id, f.path("path").asText(), claim, 0, null, false);
            // A retry replaces only this transfer's unexposed partial file and checks its whole content again.
            try (OutputStream output = Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, LinkOption.NOFOLLOW_LINKS)) {
                remote.read(config.path("unitType").asText().equals("FILE") ? source : source + "/" + f.path("path").asText(), output);
            }
            long size = Files.size(path);
            String actual = digest(path);
            db.fileProgress(id, f.path("path").asText(), claim, size == f.path("size").asLong() ? size : 0, actual, false);
            if (size != f.path("size").asLong()) throw new InvalidSource("SIZE_MISMATCH");
            String expected = f.path("expectedHash").asText(null);
            if (config.path("validation").asText().equals("HASH") && (expected == null || !actual.equals(expected))) throw new InvalidSource("HASH_MISMATCH");
            db.fileProgress(id, f.path("path").asText(), claim, size, actual, true);
        }
    }
    private void publish(String id, Path temporary, Path pending, JsonNode observation) throws IOException {
        Path marker = pending.resolve(".sharing-complete");
        byte[] content = (id + "\n" + TaskWorker.sha256(observation.toString()) + "\n").getBytes(StandardCharsets.UTF_8);
        if (Files.exists(marker, LinkOption.NOFOLLOW_LINKS)) {
            verifyLocal(id, pending, observation); verifyMarker(marker, content); return;
        }
        if (!Files.exists(pending, LinkOption.NOFOLLOW_LINKS)) {
            verifyLocal(id, temporary, observation);
            try { Files.move(temporary, pending, StandardCopyOption.ATOMIC_MOVE); }
            catch (AtomicMoveNotSupportedException error) { directory(pending.getParent(), pending.getFileName().toString()); }
        }
        ControlledPaths.localDirectory(pending.toString());
        // The fallback exposes data only when the completion marker is written last and consumed by the recipient.
        boolean complete;
        try { verifyLocal(id, pending, observation); complete = true; }
        catch (IOException error) { complete = false; }
        if (!complete) {
            verifyLocal(id, temporary, observation);
            for (JsonNode dir : observation.path("directories")) local(pending, dir.asText(), true);
            for (JsonNode f : observation.path("files")) {
                Path target = local(pending, f.path("path").asText(), false);
                if (Files.exists(target, LinkOption.NOFOLLOW_LINKS) && !Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) throw new IOException("LOCAL_SPECIAL_FILE");
                Path from = local(temporary, f.path("path").asText(), false);
                if (!Files.exists(target, LinkOption.NOFOLLOW_LINKS) || !digest(target).equals(digest(from)))
                    Files.copy(from, target, StandardCopyOption.REPLACE_EXISTING, LinkOption.NOFOLLOW_LINKS);
            }
            verifyLocal(id, pending, observation);
        }
        Files.write(marker, content, StandardOpenOption.CREATE_NEW, LinkOption.NOFOLLOW_LINKS);
    }
    private static void verifyMarker(Path marker, byte[] content) throws IOException {
        if (!Files.isRegularFile(marker, LinkOption.NOFOLLOW_LINKS) || Files.size(marker) != content.length
                || !Arrays.equals(Files.readAllBytes(marker), content)) throw new IOException("PUBLICATION_MARKER_CONFLICT");
    }
    private void verifyLocal(String id, Path root, JsonNode observation) throws IOException {
        ControlledPaths.localDirectory(root.toString());
        Set<String> expectedPaths = new HashSet<>();
        for (JsonNode d : observation.path("directories")) expectedPaths.add(d.asText());
        for (var f : db.rows("SELECT * FROM pull_file WHERE transfer_id=?", id)) {
            String relative = (String) f.get("relative_path"); expectedPaths.add(relative);
            Path path = local(root, relative, false);
            if (!(Boolean) f.get("verified") || Files.size(path) != ((Number) f.get("size_bytes")).longValue()
                    || !digest(path).equals(f.get("actual_hash"))) throw new IOException("PUBLICATION_CONTENT_MISMATCH");
        }
        try (var paths = Files.walk(root)) {
            Iterator<Path> iterator = paths.iterator();
            while (iterator.hasNext()) {
                Path p = iterator.next(); if (p.equals(root)) continue;
                String relative = root.relativize(p).toString().replace('\\', '/');
                if (Files.isSymbolicLink(p) || (!Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS) && !Files.isDirectory(p, LinkOption.NOFOLLOW_LINKS))
                        || (!relative.equals(".sharing-complete") && !expectedPaths.contains(relative))) throw new IOException("PUBLICATION_EXTRA_OR_SPECIAL_FILE");
            }
        }
    }

    private void notification(String id, boolean success, String error) {
        var t = transfer(id);
        try {
            JsonNode config = json.readTree((String) t.get("config"));
            var task = db.one("SELECT external_task_id FROM transfer_task WHERE id=?", t.get("owner_task_id"));
            String noticeId = UUID.randomUUID().toString();
            ObjectNode notice = json.createObjectNode();
            notice.put("notificationId", noticeId); notice.put("taskRecordId", (String) t.get("owner_task_id"));
            notice.put("taskId", (String) task.get("external_task_id")); notice.put("transferId", id);
            notice.put("sourceVersion", (String) t.get("source_version")); notice.put("stage", "PULL_RESULT");
            notice.put("result", success ? "TECHNICALLY_PASSED" : "FAILED"); notice.put("unitType", config.path("unitType").asText());
            if (success) {
                notice.put("dataLocation", (String) t.get("data_location")); notice.put("validationLevel", config.path("validation").asText());
                notice.put("versionBasis", (String) t.get("version_basis")); notice.put("versionStrength", (String) t.get("version_strength"));
                ArrayNode evidence = notice.putArray("files"); long bytes = 0;
                for (var f : db.rows("SELECT * FROM pull_file WHERE transfer_id=? ORDER BY relative_path", id)) {
                    ObjectNode item = evidence.addObject(); item.put("path", (String) f.get("relative_path"));
                    long size = ((Number) f.get("size_bytes")).longValue(); bytes = Math.addExact(bytes, size); item.put("size", size);
                    item.put("actualHash", (String) f.get("actual_hash")); item.put("expectedHash", (String) f.get("expected_hash"));
                }
                notice.put("totalFiles", evidence.size()); notice.put("totalBytes", bytes);
                JsonNode observed = json.readTree((String) t.get("observation"));
                notice.put("controlFiles", observed.path("controlFiles").asInt()); notice.put("controlBytes", observed.path("controlBytes").asLong());
                notice.put("completionMarker", ".sharing-complete");
            } else notice.put("errorType", error);
            String payload = notice.toString();
            outbox.insertNotification(noticeId, (String) t.get("owner_task_id"), "PULL:" + id, success ? "TECHNICALLY_PASSED" : "FAILED",
                    "JSON", config.path("notificationUrl").asText(), payload, TaskWorker.sha256(payload));
            db.update("UPDATE transfer_notification SET transfer_id=? WHERE task_id=? AND stage=?", id, t.get("owner_task_id"), "PULL:" + id);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) { throw new IllegalStateException(e); }
    }

    public Map<String, Object> confirm(String recipient, ObjectNode receipt) {
        Set<String> fields = Set.of("transferId", "sourceVersion", "result", "reason", "category");
        receipt.fieldNames().forEachRemaining(field -> { if (!fields.contains(field)) throw TaskService.error(HttpStatus.BAD_REQUEST, "INVALID_CONFIRMATION"); });
        String id = TaskService.required(receipt, "transferId", 36), version = TaskService.required(receipt, "sourceVersion", 64);
        String result = TaskService.required(receipt, "result", 10);
        if (!Set.of("ACCEPTED", "REJECTED").contains(result)) throw TaskService.error(HttpStatus.BAD_REQUEST, "INVALID_CONFIRMATION");
        String reason = receipt.hasNonNull("reason") ? TaskService.required(receipt, "reason", 1000) : null;
        String category = receipt.hasNonNull("category") ? TaskService.required(receipt, "category", 100) : null;
        if (result.equals("REJECTED") && (reason == null || category == null)) throw TaskService.error(HttpStatus.BAD_REQUEST, "INVALID_CONFIRMATION");
        String outcome = tx.execute(status -> {
            List<Map<String, Object>> matches = db.rows("SELECT * FROM pull_transfer WHERE id=?", id);
            if (matches.isEmpty()) throw TaskService.error(HttpStatus.NOT_FOUND, "TRANSFER_NOT_FOUND");
            var transfer = matches.get(0);
            JsonNode config;
            try { config = json.readTree((String) transfer.get("config")); }
            catch (com.fasterxml.jackson.core.JsonProcessingException error) { throw new IllegalStateException(error); }
            if (!recipient.equals(config.path("recipientId").asText())) throw TaskService.error(HttpStatus.FORBIDDEN, "RECIPIENT_FORBIDDEN");
            String current = (String) db.one("SELECT current_transfer_id FROM pull_delivery_key WHERE delivery_key=? FOR UPDATE", transfer.get("delivery_key")).get("current_transfer_id");
            transfer = db.one("SELECT * FROM pull_transfer WHERE id=? FOR UPDATE", id);
            String previous = (String) transfer.get("acceptance_status");
            String disposition;
            if (!version.equals(transfer.get("source_version"))) disposition = "CONFLICT";
            else if (!id.equals(current)) disposition = "HISTORICAL";
            else if (!"PUBLISHED".equals(transfer.get("status")) || transfer.get("published_at") == null) disposition = "CONFLICT";
            else if (previous.equals(result) && Objects.equals(reason, transfer.get("acceptance_reason"))
                    && Objects.equals(category, transfer.get("acceptance_category"))) disposition = "DUPLICATE";
            else if (Set.of("ACCEPTED", "REJECTED").contains(previous)) disposition = "CONFLICT";
            else {
                db.update("UPDATE pull_transfer SET acceptance_status=?,acceptance_reason=?,acceptance_category=? WHERE id=?", result, reason, category, id);
                disposition = "RECEIVED";
            }
            db.update("INSERT INTO pull_receipt(transfer_id,recipient_id,payload,outcome) VALUES (?,?,?,?)", id, recipient, receipt.toString(), disposition);
            List<Map<String, Object>> tasks = db.rows("SELECT task_id FROM pull_task_unit WHERE transfer_id=? ORDER BY task_id", id);
            for (var task : tasks) db.aggregate((String) task.get("task_id"), null, config.path("observationSeconds").asInt());
            return disposition;
        });
        if (outcome.equals("CONFLICT")) throw TaskService.error(HttpStatus.CONFLICT, "CONFIRMATION_CONFLICT");
        return Map.of("received", true, "transferId", id, "outcome", outcome);
    }

    /** Operator-only service entry; transport retries and HTTP endpoints never call this. */
    public String redeliver(String id, String repairReason) throws Exception {
        if (repairReason == null || repairReason.isBlank() || repairReason.length() > 1000) throw new IllegalArgumentException("REPAIR_REASON_REQUIRED");
        var old = transfer(id);
        JsonNode config = json.readTree((String) old.get("config")), frozen = json.readTree((String) old.get("observation"));
        var task = db.one("SELECT endpoint_revision_id FROM transfer_task WHERE id=?", old.get("owner_task_id"));
        EndpointRevision endpoint = revisions.findById((String) task.get("endpoint_revision_id")).orElseThrow();
        try (RemoteFileClient remote = clients.connect(endpoint)) {
            if (!observe(remote, (String) old.get("source_path"), config).toString().equals(frozen.toString())) throw new Changed();
        }
        return tx.execute(status -> {
            String current = (String) db.one("SELECT current_transfer_id FROM pull_delivery_key WHERE delivery_key=? FOR UPDATE", old.get("delivery_key")).get("current_transfer_id");
            var locked = db.one("SELECT status,claim_id FROM pull_transfer WHERE id=? FOR UPDATE", id);
            if (!id.equals(current) || locked.get("claim_id") != null || !Set.of("PUBLISHED", "FAILED").contains(locked.get("status")))
                throw TaskService.error(HttpStatus.CONFLICT, "REDELIVERY_CONFLICT");
            String next = create((String) old.get("owner_task_id"), (String) old.get("endpoint_id"), (String) old.get("source_path"),
                    (String) old.get("source_version"), (String) old.get("version_basis"), (String) old.get("observation"), (String) old.get("config"), (String) old.get("delivery_key"), id);
            db.update("INSERT INTO pull_receipt(transfer_id,recipient_id,payload,outcome) VALUES (?,'OPERATOR',?,'REDELIVERED')", id, repairReason);
            for (var linked : db.rows("SELECT task_id,source_path FROM pull_task_unit WHERE transfer_id=? ORDER BY task_id", id)) {
                String taskId = (String) linked.get("task_id");
                db.one("SELECT id FROM transfer_task WHERE id=? FOR UPDATE", taskId);
                link(taskId, (String) linked.get("source_path"), next);
                db.aggregate(taskId, null, config.path("observationSeconds").asInt());
                db.update("UPDATE transfer_task SET next_run_at=now() WHERE id=?", taskId);
            }
            return next;
        });
    }

    private void remind(String task, int seconds) {
        db.update("UPDATE pull_transfer SET acceptance_reminded_at=now() WHERE id IN (SELECT transfer_id FROM pull_task_unit WHERE task_id=?) AND acceptance_status='PENDING' AND published_at<now()-(? * interval '1 second') AND acceptance_reminded_at IS NULL", task, seconds);
    }
    private void unitFailure(Task task, String path, Exception error) {
        tx.executeWithoutResult(status -> {
            db.update("UPDATE pull_task_unit SET attempts=attempts+1,error_type=? WHERE task_id=? AND source_path=?", reason(error), task.taskRecordId, path);
            db.update("UPDATE pull_candidate SET last_error=? WHERE source_id=(SELECT cast(pull_config AS json)->>'sourceId' FROM transfer_task WHERE id=?) AND source_path=?", reason(error), task.taskRecordId, path);
            var unit = db.one("SELECT attempts FROM pull_task_unit WHERE task_id=? AND source_path=?", task.taskRecordId, path);
            if (error instanceof InvalidSource || error instanceof IllegalArgumentException || ((Number) unit.get("attempts")).intValue() >= 4) {
                db.update("UPDATE pull_task_unit SET status='FAILED' WHERE task_id=? AND source_path=?", task.taskRecordId, path);
                JsonNode config;
                try { config = json.readTree((String) db.one("SELECT pull_config FROM transfer_task WHERE id=?", task.taskRecordId).get("pull_config")); }
                catch (com.fasterxml.jackson.core.JsonProcessingException e) { throw new IllegalStateException(e); }
                String noticeId = UUID.randomUUID().toString();
                ObjectNode notice = json.createObjectNode().put("notificationId", noticeId).put("taskRecordId", task.taskRecordId)
                        .put("taskId", task.taskId).put("stage", "PULL_RESULT").put("result", "FAILED").put("sourcePath", path).put("errorType", reason(error));
                String payload = notice.toString();
                outbox.insertNotification(noticeId, task.taskRecordId, "PULL_UNIT:" + TaskWorker.sha256(path), "FAILED", "JSON",
                        config.path("notificationUrl").asText(), payload, TaskWorker.sha256(payload));
            }
        });
    }
    private Map<String, Object> transfer(String id) { return db.one("SELECT * FROM pull_transfer WHERE id=?", id); }
    private static String reason(Exception error) { return error.getMessage() != null && error.getMessage().matches("[A-Z][A-Z0-9_]{2,80}") ? error.getMessage() : error.getClass().getSimpleName(); }
    private static String location(JsonNode config, String id) { return config.path("accessBase").asText().replaceAll("/+$", "") + "/pending/" + id + "/"; }
    private static String control(String path, boolean file, String name) { return path + (file ? "." : "/") + name; }
    private static boolean isControl(String name, JsonNode config) {
        return name.equals(config.path("manifestName").asText()) || name.equals(config.path("readyName").asText())
                || (config.path("unitType").asText().equals("FILE") && ((config.hasNonNull("manifestName") && name.endsWith("." + config.path("manifestName").asText()))
                    || (config.hasNonNull("readyName") && name.endsWith("." + config.path("readyName").asText()))));
    }
    private static boolean exists(RemoteFileClient remote, String path, String root) throws IOException {
        if (!ControlledPaths.within(root, path)) throw new InvalidSource("PATH_OUTSIDE_ROOT");
        if (path.equals(root)) return true;
        String parent = path.substring(0, path.lastIndexOf('/')); if (parent.isEmpty()) parent = "/";
        if (!exists(remote, parent, root)) return false;
        String name = path.substring(path.lastIndexOf('/') + 1);
        for (RemoteFileClient.Entry e : remote.list(parent)) if (e.name.equals(name)) { ordinary(e); return true; }
        return false;
    }
    private static void ordinary(RemoteFileClient.Entry entry) throws InvalidSource {
        if (entry.link || (!entry.regular && !entry.directory)) throw new InvalidSource("LINK_OR_SPECIAL_FILE");
    }
    private static byte[] controlBytes(RemoteFileClient remote, String path) throws IOException {
        var entry = remote.lstat(path); ordinary(entry);
        if (!entry.regular || entry.size > 1048576) throw new InvalidSource("INVALID_CONTROL_FILE");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        remote.read(path, new FilterOutputStream(bytes) {
            int count;
            @Override public void write(int b) throws IOException { if (++count > 1048576) throw new InvalidSource("CONTROL_TOO_LARGE"); out.write(b); }
            @Override public void write(byte[] b, int off, int len) throws IOException { if ((count += len) > 1048576) throw new InvalidSource("CONTROL_TOO_LARGE"); out.write(b, off, len); }
        });
        if (bytes.size() != entry.size) throw new Changed();
        return bytes.toByteArray();
    }
    private static void safeName(String value) throws InvalidSource {
        try { ControlledPaths.relative(value); }
        catch (IllegalArgumentException error) { throw new InvalidSource("INVALID_SOURCE_PATH"); }
        if (value.contains("/") || value.matches(".*[<>:\"|?*].*") || value.endsWith(".") || value.endsWith(" ")
                || value.startsWith(".sharing-") || value.matches("(?i)(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\\..*)?")) throw new InvalidSource("INVALID_SOURCE_NAME");
    }
    private static void safeRelative(String value) throws InvalidSource { for (String part : value.split("/", -1)) safeName(part); }
    private static Path directory(Path root, String name) throws IOException {
        safeName(name); ControlledPaths.localDirectory(root.toString()); Path path = root.resolve(name);
        try { Files.createDirectory(path); } catch (FileAlreadyExistsException ignored) { }
        return ControlledPaths.localDirectory(path.toString());
    }
    private static Path local(Path root, String relative, boolean dir) throws IOException {
        safeRelative(relative); String[] parts = relative.split("/"); Path path = root;
        ControlledPaths.localDirectory(root.toString());
        for (int i = 0; i < parts.length; i++) {
            if (i < parts.length - 1 || dir) path = directory(path, parts[i]);
            else path = path.resolve(parts[i]);
        }
        if (Files.isSymbolicLink(path)) throw new InvalidSource("LOCAL_LINK");
        return path;
    }
    private static MessageDigest sha() { try { return MessageDigest.getInstance("SHA-256"); } catch (NoSuchAlgorithmException error) { throw new IllegalStateException(error); } }
    private static String hex(byte[] bytes) { StringBuilder result = new StringBuilder(); for (byte b : bytes) result.append(String.format("%02x", b & 255)); return result.toString(); }
    private static String digest(Path path) throws IOException {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) throw new InvalidSource("LOCAL_SPECIAL_FILE");
        MessageDigest digest = sha();
        try (InputStream input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
            byte[] bytes = new byte[65536]; int count;
            while ((count = input.read(bytes)) != -1) digest.update(bytes, 0, count);
        }
        return hex(digest.digest());
    }
    static class InvalidSource extends IOException { InvalidSource(String reason) { super(reason); } }
    static class NotReady extends IOException { NotReady(String reason) { super(reason); } }
    static class Changed extends IOException { Changed() { super("SOURCE_CHANGED"); } }
}
