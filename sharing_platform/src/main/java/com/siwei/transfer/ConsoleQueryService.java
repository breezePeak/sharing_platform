package com.siwei.transfer;

import com.siwei.endpoint.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ConsoleQueryService {
    public static final int PAGE_SIZE = 25;
    private static final String TASK = "SELECT t.id,t.external_task_id,t.endpoint_id,t.listener_id,t.listener_revision,t.trigger_type,t.caller_id,"
            + "t.direction,t.status,t.phase,t.acceptance_status,t.error_type,t.error_detail,t.created_at,t.updated_at,t.next_run_at,t.attempts,"
            + "t.current_file,t.read_only_recheck,t.reconcile_attempts,t.push_publish_intent,t.push_target_root,t.push_source_root,t.push_manifest_sha256,"
            + "t.pull_scan_complete,(t.pull_config IS NOT NULL) AS has_pull_config,r.name AS endpoint_name,r.protocol,r.host,r.port,r.revision,"
            + "r.read_root,r.write_root,cast(t.push_route_snapshot AS json)->>'version' AS route_version,"
            + "cast(t.pull_config AS json)->>'recipientId' AS recipient_id,cast(t.pull_config AS json)->>'targetRef' AS target_ref,"
            + "coalesce(cast(t.pull_config AS json)->>'requestPath',cast(t.canonical_request AS json)->>'sourcePath') AS source_path,"
            + "cast(t.canonical_request AS json)->>'targetPath' AS requested_target,"
            + "coalesce(cast(t.pull_config AS json)->>'sourceId',cast(t.canonical_request AS json)->>'sourceCode',t.caller_id) AS source "
            + "FROM transfer_task t JOIN endpoint_revision r ON r.id=t.endpoint_revision_id ";
    private static final String NOTICES = "(n.task_id=? OR n.transfer_id IN (SELECT transfer_id FROM pull_task_transfer WHERE task_id=?))";

    private final PullStore db;
    private final PullListenerService listeners;
    private final PullSettings pulls;
    private final TransferSettings transfers;

    public ConsoleQueryService(PullStore db, PullListenerService listeners, PullSettings pulls, TransferSettings transfers) {
        this.db = db; this.listeners = listeners; this.pulls = pulls; this.transfers = transfers;
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> services() {
        var rows = db.rows("SELECT e.id,e.enabled,r.name,r.protocol,r.host,r.port,r.revision,p.status AS probe_status,p.can_read,"
                + "p.can_write,p.verified,p.can_rename,p.details,p.finished_at,p.started_at,"
                + "coalesce(p.revision_id<>r.id OR p.finished_at<now()-interval '15 minutes',false) AS stale "
                + "FROM endpoint e JOIN endpoint_revision r ON r.endpoint_id=e.id AND r.revision=e.revision "
                + "LEFT JOIN LATERAL (SELECT * FROM endpoint_probe WHERE endpoint_id=e.id ORDER BY started_at DESC LIMIT 1) p ON true "
                + "ORDER BY r.name,e.id");
        Map<Object, List<Map<String, Object>>> grouped = new HashMap<>();
        for (var listener : listeners.list()) grouped.computeIfAbsent(listener.get("endpoint_id"), key -> new ArrayList<>()).add(listener);
        for (var row : rows) row.put("listeners", grouped.getOrDefault(row.get("id"), List.of()));
        return rows;
    }

    @Transactional(readOnly = true)
    public Map<String, Object> configuration() {
        var sources = new ArrayList<Map<String, Object>>();
        for (var entry : new TreeMap<>(pulls.getSources()).entrySet()) {
            var s = entry.getValue();
            var row = new LinkedHashMap<String, Object>();
            row.put("id", entry.getKey()); row.put("endpoint", s.getEndpointId()); row.put("path", s.getSourcePath());
            row.put("unit", s.getUnitType()); row.put("validation", s.getValidation()); row.put("target", s.getTargetRef());
            row.put("recipient", s.getRecipientId()); row.put("callers", s.getCallerIds());
            row.put("revisions", db.one("SELECT count(*) AS total FROM pull_source_revision WHERE source_id=?", entry.getKey()).get("total"));
            try { listeners.validateConfiguredSource(entry.getKey()); row.put("valid", true); }
            catch (RuntimeException error) { row.put("valid", false); }
            sources.add(row);
        }
        var routes = new ArrayList<Map<String, Object>>();
        var recipients = new ArrayList<Map<String, Object>>();
        for (var entry : new TreeMap<>(transfers.getCallers()).entrySet()) {
            var caller = entry.getValue();
            recipients.add(Map.of("caller", entry.getKey(), "endpoints", caller.getEndpointIds(),
                    "configured", validNotification(caller.getNotificationUrl())));
            for (var item : new TreeMap<>(caller.getRoutes()).entrySet()) {
                var route = item.getValue();
                var row = new LinkedHashMap<String, Object>();
                row.put("caller", entry.getKey()); row.put("type", item.getKey()); row.put("version", route.getVersion());
                row.put("endpoint", route.getEndpointId()); row.put("source", route.getSourceRoot()); row.put("target", route.getTargetRoot());
                row.put("enabled", route.isEnabled()); row.put("configured", validNotification(
                        route.getNotificationUrl() == null ? caller.getNotificationUrl() : route.getNotificationUrl()));
                routes.add(row);
            }
        }
        return Map.of("sources", sources, "routes", routes, "recipients", recipients);
    }

    private static boolean validNotification(String value) {
        if (value == null) return false;
        try {
            var url = java.net.URI.create(value);
            return ("https".equals(url.getScheme()) || "http".equals(url.getScheme())) && url.getHost() != null
                    && url.getUserInfo() == null && url.getFragment() == null;
        } catch (IllegalArgumentException error) { return false; }
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> tasks(String direction, String endpoint, String status, Instant since, Instant until, int page) {
        StringBuilder sql = new StringBuilder(TASK).append("WHERE true");
        List<Object> args = new ArrayList<>();
        if (!direction.isBlank()) { sql.append(" AND t.direction=?"); args.add(direction); }
        if (!endpoint.isBlank()) { sql.append(" AND t.endpoint_id=?"); args.add(endpoint); }
        if (!status.isBlank()) { sql.append(" AND t.status=?"); args.add(status); }
        if (since != null) { sql.append(" AND t.created_at>=?"); args.add(Timestamp.from(since)); }
        if (until != null) { sql.append(" AND t.created_at<?"); args.add(Timestamp.from(until)); }
        sql.append(" ORDER BY t.created_at DESC,t.id LIMIT ? OFFSET ?");
        args.add(PAGE_SIZE + 1); args.add((long) PAGE_SIZE * page);
        var rows = db.rows(sql.toString(), args.toArray());
        for (var row : rows) status(row);
        return rows;
    }

    @Transactional(readOnly = true)
    public Map<String, Object> task(String id) {
        var row = require(TASK + "WHERE t.id=?", id);
        status(row);
        return row;
    }

    private void status(Map<String, Object> row) {
        String id = (String) row.get("id");
        boolean push = "PUSH".equals(row.get("direction"));
        Map<String, Object> totals = push
                ? db.one("SELECT count(*) AS files,coalesce(sum(size_bytes),0) AS bytes,coalesce(sum(effective_uploaded_bytes),0) AS sent,"
                        + "count(*) FILTER (WHERE verified OR (size_bytes>0 AND effective_uploaded_bytes=size_bytes)) AS sent_files,"
                        + "count(*) FILTER (WHERE verified) AS verified_files,coalesce(sum(size_bytes) FILTER (WHERE verified),0) AS verified_bytes,"
                        + "coalesce(sum(readback_bytes),0) AS readback,coalesce(sum(greatest(upload_traffic_bytes-uploaded_bytes,0)),0) AS retransmitted "
                        + "FROM transfer_file WHERE task_id=?", id)
                : db.one("SELECT count(*) AS files,coalesce(sum(f.size_bytes),0) AS bytes,coalesce(sum(f.downloaded_bytes),0) AS sent,"
                        + "count(*) FILTER (WHERE f.verified OR (f.size_bytes>0 AND f.downloaded_bytes=f.size_bytes)) AS sent_files,"
                        + "count(*) FILTER (WHERE f.verified) AS verified_files,coalesce(sum(f.size_bytes) FILTER (WHERE f.verified),0) AS verified_bytes "
                        + "FROM pull_task_unit u JOIN pull_file f ON f.transfer_id=u.transfer_id WHERE u.task_id=?", id);
        Map<String, Object> units = push ? Map.of() : db.one("SELECT count(*) FILTER (WHERE u.transfer_id IS NULL) AS unresolved,"
                + "count(*) FILTER (WHERE u.status='FAILED' OR x.status='FAILED' OR x.acceptance_status='REJECTED') AS failed,"
                + "count(*) FILTER (WHERE x.status='PUBLISHING') AS publishing "
                + "FROM pull_task_unit u LEFT JOIN pull_transfer x ON x.id=u.transfer_id WHERE u.task_id=?", id);
        Map<String, Object> notices = db.one("SELECT count(*) AS total,count(*) FILTER (WHERE status<>'ACKED') AS pending,"
                + "count(*) FILTER (WHERE status='NEEDS_ATTENTION') AS failed FROM transfer_notification n WHERE " + NOTICES, id, id);
        boolean known = push ? row.get("push_manifest_sha256") != null
                : Boolean.TRUE.equals(row.get("pull_scan_complete")) && number(units, "unresolved") == 0;
        row.putAll(totals);
        row.put("known", known);
        BigDecimal bytes = decimal(totals, "bytes");
        row.put("percent", known && bytes.signum() > 0
                ? decimal(totals, "sent").multiply(BigDecimal.valueOf(100)).divide(bytes, 0, RoundingMode.DOWN).min(BigDecimal.valueOf(100)).intValue()
                : null);
        row.put("notices", notices);
        row.put("sampled_at", Instant.now());
        row.put("publication_attention", Set.of("RECONCILING", "WAITING_SOURCE").contains(row.get("status"))
                && number(row, "reconcile_attempts") >= TransferJpa.RECONCILE_LIMIT && !Boolean.TRUE.equals(row.get("read_only_recheck")));
        row.put("elapsed", Math.max(0, Duration.between(((Timestamp) row.get("created_at")).toInstant(),
                (Set.of("SUCCEEDED", "FAILED", "REJECTED", "NO_NEW_DATA").contains(row.get("status"))
                        ? ((Timestamp) row.get("updated_at")).toInstant() : Instant.now())).getSeconds()));
        row.put("recheck", !Boolean.TRUE.equals(row.get("read_only_recheck")) && (push
                ? "RECONCILING".equals(row.get("status")) && Boolean.TRUE.equals(row.get("push_publish_intent"))
                : "WAITING_SOURCE".equals(row.get("status")) && number(units, "publishing") > 0));
        row.put("source_wait", null);
        if (!push) {
            var active = db.rows("SELECT x.current_file,x.attempts,x.error_type FROM pull_task_unit u JOIN pull_transfer x ON x.id=u.transfer_id "
                    + "WHERE u.task_id=? AND x.status IN ('DOWNLOADING','PUBLISHING') ORDER BY x.claimed_at DESC NULLS LAST,x.id LIMIT 1", id);
            if (!active.isEmpty()) {
                var file = active.get(0);
                row.put("current_file", file.get("current_file"));
                row.put("attempts", Math.max(number(row, "attempts"), number(file, "attempts")));
            }
            var waits = db.rows("SELECT source_path,error_type FROM pull_task_unit WHERE task_id=? AND transfer_id IS NULL AND error_type IS NOT NULL ORDER BY source_path LIMIT 1", id);
            row.put("source_wait", waits.isEmpty() ? null : waits.get(0));
        }
        row.put("display", label(row, push, units, notices));
    }

    private static String label(Map<String, Object> task, boolean push, Map<String, Object> units, Map<String, Object> notices) {
        String status = (String) task.get("status"), phase = (String) task.get("phase");
        if ("RECONCILING".equals(status) || (!push && number(units, "publishing") > 0)) return "发布结果待核对";
        if (!push && number(units, "failed") > 0 && Set.of("RUNNING", "WAITING_SOURCE", "WAITING_ACCEPTANCE").contains(status))
            return "部分单元失败，其他单元处理中";
        if ("WAITING_ACCEPTANCE".equals(status)) return "下载及技术检查通过，待平台验收";
        if ("WAITING_SOURCE".equals(status)) return "等待源数据就绪";
        if ("REJECTED".equals(task.get("acceptance_status"))) return "平台验收拒绝";
        if ("SUCCEEDED".equals(status) && push && number(notices, "pending") > 0) return "交付成功，完成通知待送达";
        if ("RUNNING".equals(status)) {
            if ("VERIFYING".equals(phase)) return "目标核验中";
            if ("PUBLISHING".equals(phase)) return "发布中";
            if ("TRANSFERRING".equals(phase)) return "传输中";
        }
        return status;
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> details(String id, String direction, String view, int page) {
        long offset = (long) PAGE_SIZE * page;
        if ("timeline".equals(view)) {
            return db.rows("SELECT * FROM (SELECT n.id,n.created_at AS happened,'NOTICE' AS kind,n.stage AS event,n.status,n.last_error AS reason,"
                    + "n.attempts,n.transfer_id FROM transfer_notification n WHERE " + NOTICES
                    + " UNION ALL SELECT cast(p.id AS varchar),p.created_at,'RECEIPT',p.outcome,p.outcome,NULL,0,p.transfer_id "
                    + "FROM pull_receipt p JOIN pull_task_transfer x ON x.transfer_id=p.transfer_id WHERE x.task_id=? "
                    + "UNION ALL SELECT a.id,a.occurred_at,'OPERATION',a.action,a.result,a.actor,0,NULL "
                    + "FROM console_operation a WHERE a.object_id=? OR a.object_id IN (SELECT n.id FROM transfer_notification n WHERE " + NOTICES + ")) events "
                    + "ORDER BY happened DESC,id DESC LIMIT ? OFFSET ?", id, id, id, id, id, id, PAGE_SIZE + 1, offset);
        }
        if ("units".equals(view)) {
            if ("PUSH".equals(direction)) return db.rows("SELECT product_id,product_path,product_name FROM transfer_product "
                    + "WHERE task_id=? ORDER BY position LIMIT ? OFFSET ?", id, PAGE_SIZE + 1, offset);
            return db.rows("SELECT u.source_path,u.status AS unit_status,u.error_type AS unit_error,x.id AS transfer_id,x.source_version,x.version_basis,"
                    + "x.status,x.acceptance_status,x.error_type,x.acceptance_reason,x.data_location,x.published_at,x.created_at,x.attempts "
                    + "FROM pull_task_unit u LEFT JOIN pull_transfer x ON x.id=u.transfer_id WHERE u.task_id=? "
                    + "ORDER BY u.source_path LIMIT ? OFFSET ?", id, PAGE_SIZE + 1, offset);
        }
        if ("PULL".equals(direction)) return db.rows("SELECT f.transfer_id,f.relative_path,f.size_bytes,f.downloaded_bytes AS effective_bytes,"
                + "f.verified,f.expected_hash,f.actual_hash FROM pull_task_unit u JOIN pull_file f ON f.transfer_id=u.transfer_id "
                + "WHERE u.task_id=? ORDER BY f.transfer_id,f.relative_path LIMIT ? OFFSET ?", id, PAGE_SIZE + 1, offset);
        return db.rows("SELECT product_id,relative_path,size_bytes,effective_uploaded_bytes AS effective_bytes,verified,sha256 AS expected_hash,"
                + "upload_traffic_bytes,readback_bytes FROM transfer_file WHERE task_id=? ORDER BY product_id,relative_path LIMIT ? OFFSET ?",
                id, PAGE_SIZE + 1, offset);
    }

    public String notificationState(String task, String notice) {
        return (String) require("SELECT n.status FROM transfer_notification n WHERE " + NOTICES + " AND n.id=?", task, task, notice).get("status");
    }

    public boolean resend(String task, String notice) {
        return db.update("UPDATE transfer_notification n SET status='PENDING',next_attempt_at=now() WHERE "
                + NOTICES + " AND n.id=? AND n.status IN ('NEEDS_ATTENTION','RETRY_WAIT') AND n.claim_id IS NULL", task, task, notice) == 1;
    }

    public boolean recheck(String task) {
        return db.update("UPDATE transfer_task t SET read_only_recheck=true,next_run_at=now() WHERE t.id=? AND t.claim_id IS NULL "
                + "AND NOT t.read_only_recheck AND ((t.status='RECONCILING' AND t.direction='PUSH' AND t.push_publish_intent) OR "
                + "(t.status='WAITING_SOURCE' AND t.direction='PULL' AND EXISTS (SELECT 1 FROM pull_task_unit u JOIN pull_transfer x "
                + "ON x.id=u.transfer_id WHERE u.task_id=t.id AND x.status='PUBLISHING' AND x.publish_intent)))", task) == 1;
    }

    private Map<String, Object> require(String sql, Object... args) {
        return db.rows(sql, args).stream().findFirst().orElseThrow(() -> TaskService.error(HttpStatus.NOT_FOUND, "RECORD_NOT_FOUND"));
    }

    private static BigDecimal decimal(Map<String, Object> row, String field) { return new BigDecimal(row.get(field).toString()); }
    private static long number(Map<String, Object> row, String field) { return ((Number) row.get(field)).longValue(); }
}
