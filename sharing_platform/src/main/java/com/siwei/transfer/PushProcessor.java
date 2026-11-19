package com.siwei.transfer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.siwei.endpoint.EndpointRevision;
import com.siwei.endpoint.EndpointRevisionRepository;
import com.siwei.protocol.ControlledPaths;
import com.siwei.protocol.RemoteFileClient;
import com.siwei.protocol.RemoteFileClientFactory;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

@Component
@ConditionalOnProperty(name = "platform.transfers.push-enabled", havingValue = "true", matchIfMissing = true)
public class PushProcessor implements TaskProcessor {
    private final PushStore store;
    private final ObjectMapper json;
    private final EndpointRevisionRepository revisions;
    private final TransferSettings transferSettings;
    private final RemoteFileClientFactory clients;

    public PushProcessor(PushStore store, ObjectMapper json, EndpointRevisionRepository revisions,
                         TransferSettings transferSettings, RemoteFileClientFactory clients) {
        this.store = store; this.json = json; this.revisions = revisions; this.transferSettings = transferSettings;
        this.clients = clients;
    }

    @Override public String direction() { return "PUSH"; }

    @Override public Result process(Task task) throws Exception {
        EndpointRevision endpoint = revisions.findById(task.endpointRevisionId).orElseThrow();
        TransferTaskRecord snapshot = store.snapshot(task.taskRecordId);
        JsonNode request = json.readTree(task.canonicalRequest);
        String sourceCode = request.has("sourceCode") ? request.get("sourceCode").asText() : task.callerId;
        if (task.phase.equals("ACCEPTED")) {
            try {
                if (!sourceCode.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,99}")) throw new IOException("INVALID_SOURCE_CODE");
                String target = ControlledPaths.resolve(endpoint.writeRoot, request.get("targetPath").asText())
                        + "/" + sourceCode + "/" + task.taskRecordId;
                Path source = ControlledPaths.localDirectory(snapshot.pushSourceRoot);
                return precheck(task, endpoint, source, target);
            }
            catch (IOException | IllegalArgumentException error) {
                return new Result("REJECTED", "PRECHECK", "PRECHECK_FAILED", safeReason(error));
            }
        }
        if (!sourceCode.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,99}")) throw new IOException("INVALID_SOURCE_CODE");
        String target = ControlledPaths.resolve(endpoint.writeRoot, request.get("targetPath").asText())
                + "/" + sourceCode + "/" + task.taskRecordId;
        String frozenSource = snapshot.pushSourceRoot;
        if (frozenSource == null) throw new IllegalStateException("SOURCE_ROOT_NOT_FROZEN");
        String frozen = snapshot.pushManifestSha256;
        if (frozen == null) throw new IllegalStateException("MANIFEST_NOT_FROZEN");
        List<TransferFileRecord> files = store.files(task.taskRecordId);
        List<String> dirs = store.directories(task.taskRecordId);
        byte[] manifest = manifest(task, files, dirs);
        if (snapshot.pushManifest != null && !Arrays.equals(manifest, snapshot.pushManifest.getBytes(StandardCharsets.UTF_8)))
            throw new IllegalStateException("MANIFEST_CHANGED");
        if (!hex(hash(manifest)).equals(frozen)) throw new IllegalStateException("MANIFEST_CHANGED");
        try (RemoteFileClient remote = clients.connect(endpoint)) {
            if (snapshot.readOnlyRecheck) {
                if (!published(remote, task, target, frozen, manifest))
                    return new Result("RECONCILING", "PUBLISHING", "PUBLICATION_UNKNOWN", null);
                verifyTarget(remote, target, task, files, dirs);
                return new Result("SUCCEEDED", "COMPLETE", null, null);
            }
            recoverTemporary(remote, task, target, snapshot, manifest, ready(task, frozen));
            if (published(remote, task, target, frozen, manifest)) {
                verifyTarget(remote, target, task, files, dirs);
                return new Result("SUCCEEDED", "COMPLETE", null, null);
            }
            Path source = ControlledPaths.localDirectory(frozenSource);
            boolean intent = snapshot.pushPublishIntent;
            if (!intent) {
                verifySource(source, task, files, dirs);
                ensureDirectory(remote, endpoint.writeRoot, target);
                store.phase(task, "TRANSFERRING");
                for (String directory : dirs) ensureDirectory(remote, target, target + "/" + directory);
                for (TransferFileRecord file : files) upload(remote, source, target, task, file);
            }
            verifySource(source, task, files, dirs);
            store.phase(task, "VERIFYING");
            verifyTarget(remote, target, task, files, dirs);
            if (exists(remote, target, target + "/_manifest.xml")) verifyBytes(remote, target + "/_manifest.xml", manifest);
            else {
                store.manifestIntent(task);
                writeVerified(remote, target, target + "/_manifest.xml", manifest, target + "/.sharing-manifest-" + task.taskRecordId);
            }
            if (!intent) store.publishIntent(task);
            byte[] ready = ready(task, frozen);
            String temporary = target + "/.sharing-ready-" + task.taskRecordId;
            try {
                if (exists(remote, target, temporary)) verifyBytes(remote, temporary, ready);
                else {
                    remote.write(temporary, new ByteArrayInputStream(ready));
                    verifyBytes(remote, temporary, ready);
                }
                remote.rename(temporary, target + "/_READY");
            } catch (IOException unknown) {
                if (published(remote, task, target, frozen, manifest)) {
                    verifyTarget(remote, target, task, files, dirs);
                    return new Result("SUCCEEDED", "COMPLETE", null, null);
                }
                return new Result("RECONCILING", "PUBLISHING", "PUBLICATION_UNKNOWN", null);
            }
            if (!published(remote, task, target, frozen, manifest))
                return new Result("RECONCILING", "PUBLISHING", "PUBLICATION_UNKNOWN", null);
            verifyTarget(remote, target, task, files, dirs);
            return new Result("SUCCEEDED", "COMPLETE", null, null);
        }
    }

    private Result precheck(Task task, EndpointRevision endpoint, Path source, String target) throws Exception {
        if (endpoint.probeDirectory == null) throw new IOException("PROBE_DIRECTORY_REQUIRED");
        List<TransferFileRecord> files = scan(source, task);
        List<String> dirs = scanDirectories(source, task);
        verifySource(source, task, files, dirs);
        if (target.length() > 1000) throw new IOException("TARGET_PATH_TOO_LONG");
        for (TransferFileRecord file : files) if (target.length() + file.product.length() + file.path.length() + 2 > 1000)
            throw new IOException("TARGET_PATH_TOO_LONG");
        byte[] manifest = manifest(task, files, dirs);
        String digest = hex(hash(manifest));
        try (RemoteFileClient remote = clients.connect(endpoint)) {
            String probeRoot = ControlledPaths.resolve(endpoint.writeRoot, endpoint.probeDirectory);
            if (!remote.lstat(probeRoot).directory) throw new IOException("PROBE_DIRECTORY_REQUIRED");
            String probe = probeRoot + "/.sharing-probe-" + UUID.randomUUID().toString().replace("-", "");
            remote.mkdir(probe);
            try {
                byte[] check = "push-read-write-rename".getBytes(StandardCharsets.UTF_8);
                remote.write(probe + "/payload", new ByteArrayInputStream(check));
                verifyBytes(remote, probe + "/payload", check);
                remote.rename(probe + "/payload", probe + "/renamed");
                verifyBytes(remote, probe + "/renamed", check);
            } finally {
                try { remote.deleteTemporary(probe + "/payload", false); } catch (IOException ignored) { }
                try { remote.deleteTemporary(probe + "/renamed", false); } catch (IOException ignored) { }
                remote.deleteTemporary(probe, true);
            }
            String capacity = remote.capacity();
            if (capacity.startsWith("BYTES:") && Long.parseLong(capacity.substring(6)) < files.stream().mapToLong(f -> f.size).sum())
                throw new IOException("INSUFFICIENT_CAPACITY");
            // A task UUID owns its target directory. A pre-existing directory cannot be silently reused.
            if (exists(remote, endpoint.writeRoot, target)) throw new IOException("TARGET_ALREADY_EXISTS");
        }
        store.freeze(task, source.toString(), target, digest, new String(manifest, StandardCharsets.UTF_8), files, dirs);
        return new Result("WAITING_FEEDBACK", "PRECHECK", null, null);
    }

    List<TransferFileRecord> scan(Path source, Task task) throws IOException {
        List<TransferFileRecord> files = new ArrayList<>();
        Set<String> paths = new HashSet<>();
        Set<String> ids = new HashSet<>();
        for (Map<String, Object> product : task.products) {
            String id = (String) product.get("product_id"), relative = (String) product.get("product_path");
            if (!ids.add(id.toLowerCase(Locale.ROOT))) throw new IOException("PRODUCT_ID_CASE_COLLISION");
            Path directory = localProduct(source, relative);
            for (String existing : paths) if (relative.equalsIgnoreCase(existing) || relative.toLowerCase(Locale.ROOT).startsWith(existing.toLowerCase(Locale.ROOT) + "/")
                    || existing.toLowerCase(Locale.ROOT).startsWith(relative.toLowerCase(Locale.ROOT) + "/"))
                throw new IOException("PRODUCT_PATH_OVERLAP");
            paths.add(relative);
            int before = files.size();
            Files.walkFileTree(directory, new SimpleFileVisitor<>() {
                @Override public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attributes) throws IOException {
                    if (!attributes.isDirectory() || attributes.isSymbolicLink()) throw new IOException("LINK_OR_SPECIAL_FILE");
                    return FileVisitResult.CONTINUE;
                }
                @Override public FileVisitResult visitFile(Path path, BasicFileAttributes attributes) throws IOException {
                    if (!attributes.isRegularFile() || attributes.isSymbolicLink()) throw new IOException("LINK_OR_SPECIAL_FILE");
                    String name = path.getFileName().toString();
                    if ((!transferSettings.isAllowZeroByteFiles() && attributes.size() == 0) || name.startsWith(".sharing-")
                            || transferSettings.getTemporarySuffixes().stream().anyMatch(s -> s != null && !s.isEmpty() && name.endsWith(s)))
                        throw new IOException("EMPTY_OR_TEMPORARY_FILE");
                    String relativeName = directory.relativize(path).toString().replace('\\', '/');
                    ControlledPaths.relative(relativeName);
                    Snapshot snapshot = digest(path);
                    files.add(new TransferFileRecord(id, relativeName, snapshot.size, snapshot.sha, snapshot.modified));
                    return FileVisitResult.CONTINUE;
                }
            });
            if (files.size() == before) throw new IOException("EMPTY_PRODUCT");
        }
        files.sort(TransferFileRecord.ORDER);
        Set<String> folded = new HashSet<>();
        for (TransferFileRecord f : files) if (!folded.add((f.product + "/" + f.path).toLowerCase(Locale.ROOT)))
            throw new IOException("CASE_COLLISION");
        return files;
    }

    void verifySource(Path source, Task task, List<TransferFileRecord> expected, List<String> expectedDirs) throws IOException {
        List<TransferFileRecord> actual;
        try { actual = scan(source, task); }
        catch (IOException error) { throw new SourceChanged("SOURCE_CHANGED"); }
        if (actual.size() != expected.size()) throw new SourceChanged("SOURCE_SET_CHANGED");
        for (int i = 0; i < actual.size(); i++) if (!actual.get(i).same(expected.get(i))) throw new SourceChanged("SOURCE_CHANGED");
        try { if (!scanDirectories(source, task).equals(expectedDirs)) throw new SourceChanged("SOURCE_DIRECTORIES_CHANGED"); }
        catch (IOException error) { throw new SourceChanged("SOURCE_DIRECTORIES_CHANGED"); }
    }

    List<String> scanDirectories(Path source, Task task) throws IOException {
        List<String> dirs = new ArrayList<>();
        for (Map<String, Object> product : task.products) {
            String id = (String) product.get("product_id");
            Path base = localProduct(source, (String) product.get("product_path"));
            Files.walkFileTree(base, new SimpleFileVisitor<>() {
                @Override public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes attributes) throws IOException {
                    if (!attributes.isDirectory() || attributes.isSymbolicLink()) throw new IOException("LINK_OR_SPECIAL_FILE");
                    if (!path.equals(base) && path.getFileName().toString().startsWith(".sharing-")) throw new IOException("TEMPORARY_DIRECTORY");
                    if (!path.equals(base)) dirs.add(id + "/" + ControlledPaths.relative(base.relativize(path).toString().replace('\\', '/')));
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        Collections.sort(dirs);
        Set<String> folded = new HashSet<>();
        for (String dir : dirs) if (!folded.add(dir.toLowerCase(Locale.ROOT))) throw new IOException("DIRECTORY_CASE_COLLISION");
        return dirs;
    }

    private static Path localProduct(Path source, String relative) throws IOException {
        Path path = source;
        for (String segment : ControlledPaths.relative(relative).split("/")) {
            path = path.resolve(segment);
            BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isDirectory() || attributes.isSymbolicLink() || attributes.isOther()) throw new IOException("INVALID_PRODUCT_DIRECTORY");
        }
        if (!path.toRealPath().startsWith(source.toRealPath())) throw new IOException("PRODUCT_OUTSIDE_SOURCE");
        return path;
    }

    private void upload(RemoteFileClient remote, Path source, String target, Task task, TransferFileRecord file) throws IOException {
        String path = target + "/" + file.product + "/" + file.path;
        if (file.verified) {
            verifyRemote(remote, path, task, file);
            return;
        }
        ensureDirectory(remote, target, path.substring(0, path.lastIndexOf('/')));
        if (exists(remote, target, path)) {
            verifyRemote(remote, path, task, file);
            store.progress(task, file, file.size, 0, 0, true);
            return;
        }
        String temporary = target + "/.sharing-file-" + hex(hash((file.product + "/" + file.path).getBytes(StandardCharsets.UTF_8)));
        Path local = localProduct(source, productPath(task, file.product)).resolve(file.path);
        Snapshot before = digest(local);
        if (!before.same(file)) throw new SourceChanged("SOURCE_CHANGED");
        if (exists(remote, target, temporary)) remote.deleteTemporary(temporary, false);
        store.phase(task, "TRANSFERRING");
        store.uploadStarted(task, file);
        Checkpoint progress = new Checkpoint(task, file, true);
        try (DigestInput input = new DigestInput(Files.newInputStream(local, LinkOption.NOFOLLOW_LINKS), progress)) {
            remote.write(temporary, input);
            if (input.count != file.size || !hex(input.hash.digest()).equals(file.sha)) throw new SourceChanged("SOURCE_CHANGED_DURING_UPLOAD");
        }
        finally { progress.flush(); }
        if (!digest(local).same(file)) throw new SourceChanged("SOURCE_CHANGED");
        verifyRemote(remote, temporary, task, file);
        remote.rename(temporary, path);
        verifyRemote(remote, path, task, file);
        store.progress(task, file, file.size, 0, 0, true);
    }

    private void verifyTarget(RemoteFileClient remote, String target, Task task, List<TransferFileRecord> files, List<String> dirs) throws IOException {
        for (TransferFileRecord f : files) verifyRemote(remote, target + "/" + f.product + "/" + f.path, task, f);
        for (String dir : dirs) if (!remote.lstat(target + "/" + dir).directory) throw new IOException("TARGET_DIRECTORY_MISSING");
        Set<String> expected = new HashSet<>();
        for (TransferFileRecord f : files) expected.add(f.product + "/" + f.path);
        for (String product : expected.stream().map(p -> p.substring(0, p.indexOf('/'))).distinct().toArray(String[]::new))
            checkTree(remote, target + "/" + product, product, expected, new HashSet<>(dirs));
        for (RemoteFileClient.Entry entry : remote.list(target)) {
            if (!entry.name.equals("_manifest.xml") && !entry.name.equals("_READY")
                    && expected.stream().noneMatch(p -> p.startsWith(entry.name + "/"))) throw new IOException("EXTRA_TARGET_FILE");
        }
    }

    private void checkTree(RemoteFileClient remote, String directory, String relative, Set<String> expected, Set<String> dirs) throws IOException {
        for (RemoteFileClient.Entry entry : remote.list(directory)) {
            String name = relative + "/" + entry.name;
            if (entry.link || (!entry.directory && !entry.regular)) throw new IOException("TARGET_SPECIAL_FILE");
            if (entry.directory) {
                if (!dirs.contains(name)) throw new IOException("EXTRA_TARGET_DIRECTORY");
                checkTree(remote, directory + "/" + entry.name, name, expected, dirs);
            }
            else if (!expected.contains(name)) throw new IOException("EXTRA_TARGET_FILE");
        }
    }

    private boolean published(RemoteFileClient remote, Task task, String target, String sha, byte[] manifest) throws IOException {
        String root = store.snapshot(task.taskRecordId).pushTargetRoot;
        if (root == null || !root.equals(target)) throw new IOException("TARGET_SNAPSHOT_CHANGED");
        String configured = revisions.findById(task.endpointRevisionId).orElseThrow().writeRoot;
        if (!exists(remote, configured, target) || !exists(remote, configured, target + "/_READY")) return false;
        verifyBytes(remote, target + "/_READY", ready(task, sha));
        verifyBytes(remote, target + "/_manifest.xml", manifest);
        return true;
    }

    private void recoverTemporary(RemoteFileClient remote, Task task, String target, TransferTaskRecord snapshot,
                                  byte[] manifest, byte[] ready) throws IOException {
        if (!target.equals(snapshot.pushTargetRoot) || snapshot.pushManifestSha256 == null)
            throw new IOException("TARGET_SNAPSHOT_CHANGED");
        if (!exists(remote, revisions.findById(task.endpointRevisionId).orElseThrow().writeRoot, target)) return;
        recoverTemporary(remote, target, ".sharing-manifest-" + task.taskRecordId, manifest,
                snapshot.pushManifestIntent || snapshot.pushPublishIntent);
        recoverTemporary(remote, target, ".sharing-ready-" + task.taskRecordId, ready, snapshot.pushPublishIntent);
    }

    private static void recoverTemporary(RemoteFileClient remote, String root, String name, byte[] expected, boolean intent) throws IOException {
        String path = root + "/" + name;
        if (!exists(remote, root, path)) return;
        RemoteFileClient.Entry entry = remote.lstat(path);
        if (!intent || !entry.regular || entry.link || entry.size > expected.length)
            throw new IOException("UNOWNED_PUBLICATION_TEMPORARY");
        // Only a prefix of the exact frozen control file at the intended task location is recoverable.
        long[] count = {0};
        OutputStream prefix = new OutputStream() {
            @Override public void write(int value) throws IOException {
                if (count[0] >= expected.length || expected[(int) count[0]++] != (byte) value)
                    throw new IOException("UNOWNED_PUBLICATION_TEMPORARY");
            }
            @Override public void write(byte[] bytes, int offset, int length) throws IOException {
                for (int i = offset; i < offset + length; i++) write(bytes[i]);
            }
        };
        remote.read(path, prefix);
        if (count[0] != entry.size) throw new IOException("UNOWNED_PUBLICATION_TEMPORARY");
        remote.deleteTemporary(path, false);
    }

    private final class Checkpoint implements java.util.function.LongConsumer {
        final Task task; final TransferFileRecord file; final boolean upload;
        long bytes, pending, last = System.nanoTime();
        Checkpoint(Task task, TransferFileRecord file, boolean upload) { this.task = task; this.file = file; this.upload = upload; }
        @Override public void accept(long count) {
            bytes += count; pending += count;
            if (pending >= 8 * 1024 * 1024 || System.nanoTime() - last >= 1_000_000_000L) flush();
        }
        void flush() {
            if (pending == 0) return;
            store.progress(task, file, upload ? bytes : 0, upload ? pending : 0, upload ? 0 : pending, false);
            pending = 0; last = System.nanoTime();
        }
    }

    private static void writeVerified(RemoteFileClient remote, String root, String finalPath, byte[] bytes, String temporary) throws IOException {
        if (exists(remote, root, temporary)) remote.deleteTemporary(temporary, false);
        remote.write(temporary, new ByteArrayInputStream(bytes));
        verifyBytes(remote, temporary, bytes);
        remote.rename(temporary, finalPath);
        verifyBytes(remote, finalPath, bytes);
    }

    private void verifyRemote(RemoteFileClient remote, String path, Task task, TransferFileRecord file) throws IOException {
        store.phase(task, "VERIFYING");
        RemoteFileClient.Entry entry = remote.lstat(path);
        if (!entry.regular || entry.link || entry.size != file.size) throw new IOException("TARGET_SIZE_MISMATCH");
        Checkpoint progress = new Checkpoint(task, file, false);
        Digest sink = new Digest(progress);
        try { remote.read(path, sink); }
        finally { progress.flush(); }
        if (sink.count != file.size || !hex(sink.hash.digest()).equals(file.sha)) throw new IOException("TARGET_HASH_MISMATCH");
    }

    private static void verifyBytes(RemoteFileClient remote, String path, byte[] expected) throws IOException {
        RemoteFileClient.Entry entry = remote.lstat(path);
        if (!entry.regular || entry.link || entry.size != expected.length) throw new IOException("TARGET_SIZE_MISMATCH");
        Digest sink = new Digest(); remote.read(path, sink);
        if (sink.count != expected.length || !MessageDigest.isEqual(sink.hash.digest(), hash(expected))) throw new IOException("TARGET_HASH_MISMATCH");
    }

    private static boolean exists(RemoteFileClient remote, String root, String path) throws IOException {
        if (path.equals(root)) return true;
        int slash = path.lastIndexOf('/');
        String parent = slash == 0 ? "/" : path.substring(0, slash);
        if (!exists(remote, root, parent)) return false;
        for (RemoteFileClient.Entry e : remote.list(parent)) if (e.name.equals(path.substring(slash + 1))) return true;
        return false;
    }

    private static void ensureDirectory(RemoteFileClient remote, String root, String path) throws IOException {
        String current = root;
        String remaining = path.substring(root.length());
        for (String part : remaining.split("/")) {
            if (part.isEmpty()) continue;
            String next = current.equals("/") ? "/" + part : current + "/" + part;
            if (exists(remote, root, next)) {
                if (!remote.lstat(next).directory) throw new IOException("TARGET_NOT_DIRECTORY");
            } else remote.mkdir(next);
            current = next;
        }
    }

    private static String productPath(Task task, String id) {
        for (Map<String, Object> product : task.products) if (id.equals(product.get("product_id"))) return (String) product.get("product_path");
        throw new IllegalStateException("PRODUCT_NOT_FOUND");
    }

    static byte[] manifest(Task task, List<TransferFileRecord> files, List<String> dirs) {
        StringBuilder xml = new StringBuilder("<manifest version=\"1\" taskId=\"").append(task.taskRecordId).append("\" productCount=\"")
                .append(task.products.size()).append("\">");
        for (String dir : dirs) xml.append("<directory path=\"").append(escape(dir)).append("\"/>");
        for (TransferFileRecord f : files) xml.append("<file productId=\"").append(escape(f.product)).append("\" path=\"")
                .append(escape(f.path)).append("\" size=\"").append(f.size).append("\" sha256=\"").append(f.sha).append("\"/>");
        return xml.append("</manifest>").toString().getBytes(StandardCharsets.UTF_8);
    }
    private static byte[] ready(Task task, String sha) {
        return ("taskId=" + task.taskRecordId + "\nmanifestSha256=" + sha + "\nproductCount=" + task.products.size() + "\n").getBytes(StandardCharsets.UTF_8);
    }
    private static String escape(String value) { return value.replace("&", "&amp;").replace("\"", "&quot;").replace("<", "&lt;").replace(">", "&gt;"); }
    private static String safeReason(Exception error) {
        String message = error.getMessage();
        return message != null && message.matches("[A-Z][A-Z0-9_]{2,60}") ? message : error.getClass().getSimpleName();
    }

    private static Snapshot digest(Path path) throws IOException {
        BasicFileAttributes before = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!before.isRegularFile() || before.isSymbolicLink()) throw new IOException("LINK_OR_SPECIAL_FILE");
        Digest sink = new Digest();
        try (InputStream input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) { input.transferTo(sink); }
        BasicFileAttributes after = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!after.isRegularFile() || before.size() != after.size() || !before.lastModifiedTime().equals(after.lastModifiedTime())
                || !Objects.equals(before.fileKey(), after.fileKey()) || sink.count != after.size()) throw new IOException("SOURCE_CHANGED");
        return new Snapshot(sink.count, hex(sink.hash.digest()), after.lastModifiedTime().toInstant());
    }
    private static MessageDigest sha() { try { return MessageDigest.getInstance("SHA-256"); } catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); } }
    private static byte[] hash(byte[] bytes) { return sha().digest(bytes); }
    private static String hex(byte[] bytes) { StringBuilder out = new StringBuilder(); for (byte b : bytes) out.append(String.format("%02x", b & 255)); return out.toString(); }
    private static class Digest extends OutputStream {
        final MessageDigest hash = sha(); long count;
        final java.util.function.LongConsumer progress;
        Digest() { this(n -> {}); }
        Digest(java.util.function.LongConsumer progress) { this.progress = progress; }
        @Override public void write(int b) { hash.update((byte) b); count++; progress.accept(1); }
        @Override public void write(byte[] bytes, int offset, int length) { hash.update(bytes, offset, length); count += length; progress.accept(length); }
    }
    public static class SourceChanged extends IOException {
        SourceChanged(String reason) { super(reason); }
    }
    private static class DigestInput extends FilterInputStream {
        final MessageDigest hash = sha(); long count;
        final java.util.function.LongConsumer progress;
        DigestInput(InputStream input, java.util.function.LongConsumer progress) { super(input); this.progress = progress; }
        @Override public int read() throws IOException {
            int value = super.read();
            if (value != -1) { hash.update((byte) value); count++; progress.accept(1); }
            return value;
        }
        @Override public int read(byte[] bytes, int offset, int length) throws IOException {
            int read = in.read(bytes, offset, length);
            if (read > 0) { hash.update(bytes, offset, read); count += read; progress.accept(read); }
            return read;
        }
    }
    private static class Snapshot {
        final long size; final String sha; final Instant modified;
        Snapshot(long size, String sha, Instant modified) { this.size = size; this.sha = sha; this.modified = modified; }
        boolean same(TransferFileRecord record) { return size == record.size && sha.equals(record.sha) && modified.toEpochMilli() == record.modifiedMillis; }
    }
}
