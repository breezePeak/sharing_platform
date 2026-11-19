package com.siwei.transfer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.siwei.endpoint.*;
import com.siwei.protocol.ControlledPaths;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
public class PullListenerService {
    static final String CURRENT = "SELECT l.*,r.endpoint_id,r.source_id,r.source_path,r.config FROM pull_listener l "
            + "JOIN pull_listener_revision r ON r.listener_id=l.id AND r.revision=l.revision";
    private final PullStore db;
    private final EndpointRepository endpoints;
    private final EndpointRevisionRepository revisions;
    private final EndpointSettings roots;
    private final PullSettings pulls;
    private final TransferSettings transfers;
    private final ObjectMapper json;
    private final Clock clock;
    private final boolean scheduleEnabled;
    private final boolean engineEnabled;

    public PullListenerService(PullStore db, EndpointRepository endpoints, EndpointRevisionRepository revisions,
            EndpointSettings roots, PullSettings pulls, TransferSettings transfers, ObjectMapper json, Clock clock,
            @org.springframework.beans.factory.annotation.Value("${platform.pulls.listeners.schedule-enabled:true}") boolean scheduleEnabled,
            @org.springframework.beans.factory.annotation.Value("${platform.pulls.enabled:true}") boolean engineEnabled) {
        this.db = db; this.endpoints = endpoints; this.revisions = revisions; this.roots = roots;
        this.pulls = pulls; this.transfers = transfers; this.json = json; this.clock = clock;
        this.scheduleEnabled = scheduleEnabled; this.engineEnabled = engineEnabled;
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> list() {
        return db.rows("SELECT l.id,l.name,l.enabled,l.period_seconds,l.initial_mode,l.initialized_at,l.revision,l.edit_version,"
                + "l.next_run_at,l.manual_pending,r.endpoint_id,r.source_id,r.source_path,e.enabled AS endpoint_enabled,"
                + "s.id AS run_id,s.status AS run_status,s.error_type,s.coalesced_reason,s.started_at,s.ended_at,"
                + "s.candidate_count,s.waiting_count,s.created_count,s.reused_count,(? AND l.enabled AND l.next_run_at<=?) AS due "
                + "FROM pull_listener l JOIN pull_listener_revision r ON r.listener_id=l.id AND r.revision=l.revision "
                + "JOIN endpoint e ON e.id=r.endpoint_id LEFT JOIN LATERAL "
                + "(SELECT * FROM pull_listener_run WHERE listener_id=l.id ORDER BY started_at DESC,id DESC LIMIT 1) s ON true "
                + "ORDER BY l.name,l.id", scheduleEnabled && engineEnabled && pulls.isIntakeEnabled(), Timestamp.from(clock.instant()));
    }

    public Set<String> sourceIds() { return new TreeSet<>(pulls.getSources().keySet()); }

    @Transactional(readOnly = true)
    public Map<String, Object> form(String id) {
        var listener = require(CURRENT + " WHERE l.id=?", id);
        JsonNode config = parse(listener.get("config"));
        for (String field : List.of("unitType", "filePattern", "scanDepth", "manifestName", "readyName", "validation", "targetRef",
                "recipientId", "stableObservations", "minimumObservationSeconds"))
            listener.put(field, config.path(field).asText(null));
        return listener;
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> history(String id) {
        form(id);
        return db.rows("SELECT s.*,r.source_path,CAST(EXTRACT(epoch FROM s.ended_at-s.started_at) AS double precision) AS elapsed_seconds "
                + "FROM pull_listener_run s JOIN pull_listener_revision r ON r.listener_id=s.listener_id AND r.revision=s.revision "
                + "WHERE s.listener_id=? ORDER BY s.started_at DESC,s.id DESC LIMIT 30", id);
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> runTasks(String id, String run) {
        require("SELECT id FROM pull_listener_run WHERE listener_id=? AND id=?", id, run);
        return db.rows("SELECT t.id,t.external_task_id,t.trigger_type,t.status,t.acceptance_status,t.error_type "
                + "FROM pull_listener_run_task a JOIN transfer_task t ON t.id=a.task_id WHERE a.run_id=? "
                + "ORDER BY t.created_at,t.id", run);
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> apiTasks(String id) {
        form(id);
        return db.rows("SELECT id,external_task_id,trigger_type,status,acceptance_status,error_type "
                + "FROM transfer_task WHERE listener_id=? AND trigger_type='API' ORDER BY created_at DESC LIMIT 30", id);
    }

    @Transactional
    public String save(String id, int editVersion, String name, String source, String path, int period, String mode, boolean inherit) {
        if (name == null || name.isBlank() || name.length() > 100 || source == null || source.length() > 100
                || path == null || path.length() > 1000 || period < 1 || period > 86400
                || !Set.of("IMPORT_EXISTING", "BASELINE_ONLY").contains(mode))
            throw TaskService.error(HttpStatus.BAD_REQUEST, "INVALID_LISTENER");
        PullSettings.Source selected = pulls.getSources().get(source);
        if (selected == null) throw TaskService.error(HttpStatus.BAD_REQUEST, "SOURCE_NOT_CONFIGURED");
        Endpoint endpoint = lockEndpoint(selected.getEndpointId());
        EndpointRevision endpointRevision = current(endpoint);
        ObjectNode config = snapshot(source, path, endpointRevision);
        rejectConflictingSources(source, path, endpointRevision, config);
        rejectConflictingListeners(id, endpoint.id, path, config);
        Timestamp now = Timestamp.from(clock.instant());
        if (id == null) {
            id = UUID.randomUUID().toString();
            db.update("INSERT INTO pull_listener(id,name,period_seconds,initial_mode,revision,next_run_at) VALUES (?,?,?,?,1,?)",
                    id, name.trim(), period, mode, now);
            insertRevision(id, 1, endpoint.id, source, path, config);
            return id;
        }
        Map<String, Object> previous = require(CURRENT + " WHERE l.id=? FOR UPDATE OF l", id);
        checkVersion(previous, editVersion);
        if (!endpoint.id.equals(previous.get("endpoint_id")) || !mode.equals(previous.get("initial_mode")))
            throw TaskService.error(HttpStatus.BAD_REQUEST, "LISTENER_IDENTITY_FIXED");
        int revision = number(previous, "revision");
        boolean scopeChanged = !path.equals(previous.get("source_path")) || !config.equals(parse(previous.get("config")));
        if (scopeChanged) {
            if (!inherit) throw TaskService.error(HttpStatus.CONFLICT, "CONFIRM_SCOPE_CHANGE");
            insertRevision(id, ++revision, endpoint.id, source, path, config);
        }
        boolean reschedule = scopeChanged || period != number(previous, "period_seconds");
        db.update("UPDATE pull_listener SET name=?,period_seconds=?,revision=?,edit_version=edit_version+1,"
                + "next_run_at=CASE WHEN ? THEN cast(? AS timestamptz) ELSE next_run_at END WHERE id=?",
                name.trim(), period, revision, reschedule, now, id);
        return id;
    }

    @Transactional
    public void toggle(String id, int editVersion) {
        var listener = require("SELECT edit_version FROM pull_listener WHERE id=? FOR UPDATE", id);
        checkVersion(listener, editVersion);
        db.update("UPDATE pull_listener SET enabled=NOT enabled,edit_version=edit_version+1,"
                + "next_run_at=CASE WHEN enabled THEN next_run_at ELSE cast(? AS timestamptz) END WHERE id=?",
                Timestamp.from(clock.instant()), id);
    }

    @Transactional
    public void manual(String id) {
        if (!engineEnabled || !pulls.isIntakeEnabled())
            throw TaskService.error(HttpStatus.SERVICE_UNAVAILABLE, "ENGINE_NOT_READY");
        var listener = form(id);
        Endpoint endpoint = lockEndpoint((String) listener.get("endpoint_id"));
        if (!endpoint.enabled) throw TaskService.error(HttpStatus.BAD_REQUEST, "ENDPOINT_DISABLED");
        require("SELECT id FROM pull_listener WHERE id=? FOR UPDATE", id);
        // One pending manual scan survives capacity limits and supersedes the next due tick.
        db.update("UPDATE pull_listener SET manual_pending=true WHERE id=? AND NOT EXISTS "
                + "(SELECT 1 FROM pull_listener_run WHERE listener_id=? AND status='SCANNING' AND trigger_type='MANUAL')", id, id);
        db.update("UPDATE pull_listener_run SET coalesced_reason='MANUAL_ALREADY_RUNNING' WHERE listener_id=? "
                + "AND status='SCANNING' AND trigger_type='MANUAL'", id);
    }

    ObjectNode resolveApi(ObjectNode request, String caller, EndpointRevision endpoint) {
        String id = TaskService.required(request, "listenerId", 36);
        var listener = form(id);
        if (!endpoint.endpointId.equals(listener.get("endpoint_id")))
            throw TaskService.error(HttpStatus.BAD_REQUEST, "LISTENER_ENDPOINT_MISMATCH");
        String source = (String) listener.get("source_id");
        PullSettings.Source available = pulls.getSources().get(source);
        if (available == null || !available.getCallerIds().contains(caller))
            throw TaskService.error(HttpStatus.FORBIDDEN, "SOURCE_FORBIDDEN");
        ObjectNode config = checkedSnapshot(listener, endpoint);
        String base = (String) listener.get("source_path");
        String path = request.has("sourcePath") ? TaskService.required(request, "sourcePath", 1000) : base;
        if ((request.has("sourceId") && !source.equals(TaskService.required(request, "sourceId", 100)))
                || (request.has("targetPath") && !config.path("targetRef").asText().equals(TaskService.required(request, "targetPath", 100)))
                || !PullSettings.within(path, base)) throw TaskService.error(HttpStatus.BAD_REQUEST, "LISTENER_SCOPE_MISMATCH");
        try {
            if (ControlledPaths.resolve(endpoint.readRoot, path).length() > 1000) throw new IllegalArgumentException();
        }
        catch (IllegalArgumentException error) { throw TaskService.error(HttpStatus.BAD_REQUEST, "LISTENER_SCOPE_MISMATCH"); }
        if (!base.equals(path)) {
            int depth = config.path("scanDepth").asInt() - path.substring(base.length() + 1).split("/").length;
            String name = path.substring(path.lastIndexOf('/') + 1);
            if (depth < 0 || (depth == 0 && !PullProcessor.selected(name, config)))
                throw TaskService.error(HttpStatus.BAD_REQUEST, "LISTENER_SCOPE_MISMATCH");
            config.put("scanDepth", depth);
        }
        return config.put("requestPath", path).put("listenerRevision", number(listener, "revision"));
    }

    ObjectNode checkedSnapshot(Map<String, Object> listener, EndpointRevision endpoint) {
        String path = (String) listener.get("source_path");
        ObjectNode config = snapshot((String) listener.get("source_id"), path, endpoint);
        if (!config.equals(parse(listener.get("config"))))
            throw TaskService.error(HttpStatus.CONFLICT, "LISTENER_SCOPE_REVIEW_REQUIRED");
        rejectConflictingSources((String) listener.get("source_id"), path, endpoint, config);
        rejectConflictingListeners((String) listener.get("id"), endpoint.endpointId, path, config);
        return config;
    }

    void rejectConflictingListeners(String id, String endpoint, String path, JsonNode config) {
        for (var other : db.rows(CURRENT + " WHERE r.endpoint_id=?", endpoint))
            if (!Objects.equals(id, other.get("id")))
                compare(path, config, (String) other.get("source_path"), parse(other.get("config")));
    }

    private void rejectConflictingSources(String source, String path, EndpointRevision endpoint, JsonNode config) {
        for (var entry : pulls.getSources().entrySet()) {
            var other = entry.getValue();
            if (entry.getKey().equals(source) || !endpoint.endpointId.equals(other.getEndpointId())
                    || !Objects.equals(config.path("targetRef").asText(), other.getTargetRef())
                    || !Objects.equals(config.path("recipientId").asText(), other.getRecipientId())
                    || !Objects.equals(config.path("validation").asText(), other.getValidation())
                    || other.getSourcePath() == null || !overlap(path, other.getSourcePath())) continue;
            compare(path, config, other.getSourcePath(), snapshot(entry.getKey(), other.getSourcePath(), endpoint));
        }
    }

    private static void compare(String path, JsonNode config, String otherPath, JsonNode other) {
        String a = ControlledPaths.resolve(config.path("remoteRoot").asText(), path);
        String b = ControlledPaths.resolve(other.path("remoteRoot").asText(), otherPath);
        if (!overlap(a, b) || !PullProcessor.deliveryScope(config).equals(PullProcessor.deliveryScope(other))) return;
        boolean sameRule = PullProcessor.versionRule(config).equals(PullProcessor.versionRule(other));
        boolean sameUnitDepth = a.split("/").length + config.path("scanDepth").asInt()
                == b.split("/").length + other.path("scanDepth").asInt();
        if (!sameRule || (config.path("unitType").asText().equals("PACKAGE_DIR") && !sameUnitDepth))
            throw TaskService.error(HttpStatus.CONFLICT, "OVERLAPPING_VERSION_RULES");
    }

    private static boolean overlap(String a, String b) {
        return PullSettings.within(a, b) || PullSettings.within(b, a);
    }

    private ObjectNode snapshot(String source, String path, EndpointRevision endpoint) {
        ObjectNode config = pulls.configuredSnapshot(json, source, path, endpoint, roots, transfers);
        if (!config.hasNonNull("minimumObservationSeconds")) config.put("minimumObservationSeconds", 120);
        return config;
    }

    private void insertRevision(String id, int revision, String endpoint, String source, String path, ObjectNode config) {
        db.update("INSERT INTO pull_listener_revision(listener_id,revision,endpoint_id,source_id,source_path,config) VALUES (?,?,?,?,?,?)",
                id, revision, endpoint, source, path, config.toString());
    }

    Endpoint lockEndpoint(String id) {
        return endpoints.lockById(id).orElseThrow(() -> TaskService.error(HttpStatus.BAD_REQUEST, "ENDPOINT_NOT_FOUND"));
    }

    EndpointRevision current(Endpoint endpoint) {
        return revisions.findByEndpointIdAndRevision(endpoint.id, endpoint.revision).orElseThrow();
    }

    Map<String, Object> require(String sql, Object... args) {
        return db.rows(sql, args).stream().findFirst().orElseThrow(() -> TaskService.error(HttpStatus.NOT_FOUND, "LISTENER_NOT_FOUND"));
    }

    private JsonNode parse(Object value) {
        try { return json.readTree((String) value); }
        catch (JsonProcessingException error) { throw new IllegalStateException(error); }
    }

    private static void checkVersion(Map<String, Object> previous, int editVersion) {
        if (editVersion != number(previous, "edit_version"))
            throw new ResponseStatusException(HttpStatus.CONFLICT, "STALE_LISTENER");
    }

    static int number(Map<String, Object> row, String field) { return ((Number) row.get(field)).intValue(); }
}
