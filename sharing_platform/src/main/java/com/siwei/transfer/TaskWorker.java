package com.siwei.transfer;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import javax.annotation.PreDestroy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(TaskWorker.class);
    private final JdbcTemplate db;
    private final TaskService tasks;
    private final ObjectMapper json;
    private final TransactionTemplate tx;
    private final Map<String, TaskProcessor> processors = new HashMap<>();
    private final ConcurrentMap<String, String> active = new ConcurrentHashMap<>();
    private final ThreadPoolExecutor executor;

    public TaskWorker(JdbcTemplate db, TaskService tasks, ObjectMapper json, TransferSettings settings,
                      org.springframework.transaction.PlatformTransactionManager manager, List<TaskProcessor> processors) {
        this.db = db; this.tasks = tasks; this.json = json; this.tx = new TransactionTemplate(manager);
        for (TaskProcessor processor : processors) {
            if (this.processors.putIfAbsent(processor.direction(), processor) != null)
                throw new IllegalStateException("Duplicate task processor: " + processor.direction());
        }
        int workers = Math.max(1, Math.min(8, settings.getWorkers()));
        this.executor = new ThreadPoolExecutor(workers, workers, 0, TimeUnit.SECONDS,
                new SynchronousQueue<>(), runnable -> { Thread t = new Thread(runnable, "transfer-task"); t.setDaemon(true); return t; });
    }

    @Scheduled(fixedDelay = 1000)
    public void poll() {
        int capacity = executor.getMaximumPoolSize() - executor.getActiveCount();
        if (capacity <= 0) return;
        List<Map<String, Object>> due = db.queryForList("SELECT id,direction FROM transfer_task WHERE (status IN ('QUEUED','RECONCILING') AND next_run_at<=now()) OR (status='RUNNING' AND claimed_at<now()-interval '5 minutes') ORDER BY next_run_at LIMIT ?", capacity);
        for (Map<String, Object> row : due) {
            String id = (String) row.get("id"), direction = (String) row.get("direction");
            if (!processors.containsKey(direction)) continue;
            String claim = UUID.randomUUID().toString();
            int updated = db.update("UPDATE transfer_task SET status='RUNNING',claim_id=?,claimed_at=now(),attempts=attempts+1,version=version+1,updated_at=now() WHERE id=? AND ((status IN ('QUEUED','RECONCILING') AND next_run_at<=now()) OR (status='RUNNING' AND claimed_at<now()-interval '5 minutes'))", claim, id);
            if (updated == 0) continue;
            active.put(id, claim);
            try { executor.execute(() -> run(id, claim, processors.get(direction))); }
            catch (RejectedExecutionException e) {
                active.remove(id, claim);
                db.update("UPDATE transfer_task SET status='QUEUED',claim_id=NULL,claimed_at=NULL,attempts=attempts-1,next_run_at=now(),updated_at=now() WHERE id=? AND claim_id=? AND status='RUNNING'", id, claim);
            }
        }
    }

    private void run(String id, String claim, TaskProcessor processor) {
        Task task = null;
        TaskProcessor.Result result = null;
        try {
            String caller = db.queryForObject("SELECT caller_id FROM transfer_task WHERE id=?", String.class, id);
            task = tasks.get(caller, id);
            // shortcut: PULL still needs external-effect reconciliation after a lost result commit; add it with PR5.
            result = processor.process(task);
            if (!Set.of("SUCCEEDED", "FAILED", "REJECTED", "WAITING_FEEDBACK", "RECONCILING").contains(result.status))
                throw new IllegalArgumentException("Invalid processor result");
        } catch (Exception error) {
            try {
                int attempts = db.queryForObject("SELECT attempts FROM transfer_task WHERE id=?", Integer.class, id);
                boolean publishing = db.queryForObject("SELECT push_publish_intent FROM transfer_task WHERE id=?", Boolean.class, id);
                if (publishing) {
                    result = new TaskProcessor.Result("RECONCILING", "PUBLISHING", "PUBLICATION_UNKNOWN", null);
                } else if (error instanceof IOException && !(error instanceof PushProcessor.SourceChanged)
                        && attempts < (task != null && task.direction.equals("PUSH") ? 4 : 3)) {
                    db.update("UPDATE transfer_task SET status='QUEUED',phase='RETRY_WAIT',error_type='NETWORK_TEMP',error_detail=?,next_run_at=?,claim_id=NULL,claimed_at=NULL,updated_at=now() WHERE id=? AND claim_id=? AND status='RUNNING'",
                            safe(error), java.sql.Timestamp.from(Instant.now().plusSeconds(5L * attempts)), id, claim);
                    return;
                }
                if (task == null) {
                    String caller = db.queryForObject("SELECT caller_id FROM transfer_task WHERE id=?", String.class, id);
                    task = tasks.get(caller, id);
                }
                if (result == null) result = new TaskProcessor.Result("FAILED", "COMPLETE",
                        error instanceof IOException ? "NETWORK_TEMP" : "CONFIG", safe(error));
            } catch (Exception persistenceFailure) {
                log.warn("task retry persistence failed id={} type={}", id, persistenceFailure.getClass().getSimpleName());
                return; // Claim expires for recovery; no contradictory result is written.
            }
        } finally {
            active.remove(id, claim);
        }
        try { finish(task, claim, result); }
        catch (Exception persistenceFailure) {
            log.warn("task result persistence failed id={} type={}", id, persistenceFailure.getClass().getSimpleName());
        }
    }

    @Scheduled(fixedDelay = 30000)
    public void heartbeat() {
        active.forEach((id, claim) -> db.update("UPDATE transfer_task SET claimed_at=now() WHERE id=? AND claim_id=? AND status='RUNNING'", id, claim));
    }

    private void finish(Task task, String claim, TaskProcessor.Result result) {
        tx.executeWithoutResult(status -> {
            int updated = db.update("UPDATE transfer_task SET status=?,phase=?,error_type=?,error_detail=?,next_run_at=CASE WHEN ?='RECONCILING' THEN now()+interval '30 seconds' ELSE next_run_at END,claim_id=NULL,claimed_at=NULL,version=version+1,updated_at=now() WHERE id=? AND claim_id=? AND status='RUNNING'",
                    result.status, result.phase, result.errorType, result.errorDetail, result.status, task.taskRecordId, claim);
            if (updated == 0 || result.status.equals("RECONCILING")) return;
            String stage = result.status.equals("WAITING_FEEDBACK") || result.status.equals("REJECTED") ? "PRECHECK" : "COMPLETE";
            String recipient = db.queryForObject("SELECT notification_url FROM transfer_task WHERE id=?", String.class, task.taskRecordId);
            String notificationId = UUID.randomUUID().toString();
            String payload;
            if (task.entryType.equals("XML")) payload = LegacyXml.notification(notificationId, task, stage, result);
            else {
                var notice = json.createObjectNode();
                notice.put("notificationId", notificationId); notice.put("taskRecordId", task.taskRecordId);
                notice.put("taskId", task.taskId); notice.put("stage", stage); notice.put("result", result.status);
                if (stage.equals("COMPLETE") && task.direction.equals("PUSH")) {
                    var products = notice.putArray("products");
                    for (Map<String, Object> product : task.products) {
                        var item = products.addObject();
                        item.put("productId", (String) product.get("product_id"));
                        item.put("productPath", task.deliveryPath(product));
                        if (product.get("product_name") != null) item.put("productName", (String) product.get("product_name"));
                    }
                    notice.put("fileCount", task.products.size());
                }
                if (result.errorType != null) notice.put("errorType", result.errorType);
                if (result.errorDetail != null) notice.put("errorDetail", result.errorDetail);
                payload = notice.toString();
            }
            int inserted = db.update("INSERT INTO transfer_notification (id,task_id,stage,result_status,protocol,recipient_url,payload,payload_sha256,next_attempt_at,status,created_at) VALUES (?,?,?,?,?,?,?,?,now(),'PENDING',now()) ON CONFLICT (task_id,stage) DO NOTHING",
                    notificationId, task.taskRecordId, stage, result.status, task.entryType, recipient, payload, sha256(payload));
            if (inserted == 0 && !result.status.equals(db.queryForObject(
                    "SELECT result_status FROM transfer_notification WHERE task_id=? AND stage=?", String.class, task.taskRecordId, stage)))
                throw new IllegalStateException("Conflicting stage result");
        });
    }

    static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : digest) hex.append(String.format("%02x", b & 255));
            return hex.toString();
        } catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }

    private static String safe(Exception error) {
        return error.getClass().getSimpleName();
    }

    @PreDestroy public void close() { executor.shutdownNow(); }
}
