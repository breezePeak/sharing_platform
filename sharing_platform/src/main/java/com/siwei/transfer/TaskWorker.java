package com.siwei.transfer;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import javax.annotation.PreDestroy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(TaskWorker.class);
    private final TransferJpa db;
    private final TransferTaskRepository taskRecords;
    private final TransferNotificationRepository notificationRecords;
    private final TaskService tasks;
    private final ObjectMapper json;
    private final TransactionTemplate tx;
    private final Map<String, TaskProcessor> processors = new HashMap<>();
    private final ConcurrentMap<String, String> active = new ConcurrentHashMap<>();
    private final ThreadPoolExecutor executor;

    public TaskWorker(TransferJpa db, TransferTaskRepository taskRecords,
                      TransferNotificationRepository notificationRecords, TaskService tasks, ObjectMapper json, TransferSettings settings,
                      org.springframework.transaction.PlatformTransactionManager manager, List<TaskProcessor> processors) {
        this.db = db; this.taskRecords = taskRecords; this.notificationRecords = notificationRecords;
        this.tasks = tasks; this.json = json; this.tx = new TransactionTemplate(manager);
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
        List<Map<String, Object>> due = db.dueTasks(capacity + active.size() + 100);
        int launched = 0;
        for (Map<String, Object> row : due) {
            if (launched >= capacity) break;
            String id = (String) row.get("id"), direction = (String) row.get("direction");
            if (!processors.containsKey(direction)) continue;
            String claim = UUID.randomUUID().toString();
            if (active.putIfAbsent(id, claim) != null) continue;
            int updated;
            try { updated = db.claimTask(id, claim); }
            catch (RuntimeException e) { active.remove(id, claim); throw e; }
            if (updated == 0) { active.remove(id, claim); continue; }
            try { executor.execute(() -> run(id, claim, processors.get(direction))); launched++; }
            catch (RejectedExecutionException e) {
                active.remove(id, claim);
                db.releaseTaskClaim(id, claim);
            }
        }
    }

    private void run(String id, String claim, TaskProcessor processor) {
        try { process(id, claim, processor); }
        finally { active.remove(id, claim); }
    }

    private void process(String id, String claim, TaskProcessor processor) {
        Task task = null;
        TaskProcessor.Result result = null;
        try {
            String caller = taskRecords.findById(id).orElseThrow().callerId;
            task = tasks.get(caller, id);
            // shortcut: processors must reconcile external effects after a lost result commit; PR4/05 add publication checks.
            result = processor.process(task);
            if (!Set.of("SUCCEEDED", "FAILED", "REJECTED", "WAITING_FEEDBACK", "RECONCILING").contains(result.status))
                throw new IllegalArgumentException("Invalid processor result");
        } catch (Exception error) {
            try {
                int attempts = taskRecords.findById(id).orElseThrow().attempts;
                if (error instanceof IOException && attempts < 3) {
                    db.retryTask(id, claim, safe(error), java.sql.Timestamp.from(Instant.now().plusSeconds(5L * attempts)));
                    return;
                }
                if (task == null) {
                    String caller = taskRecords.findById(id).orElseThrow().callerId;
                    task = tasks.get(caller, id);
                }
                result = new TaskProcessor.Result("FAILED", "COMPLETE",
                        error instanceof IOException ? "NETWORK_TEMP" : "CONFIG", safe(error));
            } catch (Exception persistenceFailure) {
                log.warn("task retry persistence failed id={} type={}", id, persistenceFailure.getClass().getSimpleName());
                return; // Claim expires for recovery; no contradictory result is written.
            }
        }
        try { finish(task, claim, result); }
        catch (Exception persistenceFailure) {
            log.warn("task result persistence failed id={} type={}", id, persistenceFailure.getClass().getSimpleName());
        }
    }

    @Scheduled(fixedDelay = 30000)
    public void heartbeat() {
        active.forEach(db::heartbeatTask);
    }

    private void finish(Task task, String claim, TaskProcessor.Result result) {
        tx.executeWithoutResult(status -> {
            int updated = db.finishTask(task.taskRecordId, claim, result);
            if (updated == 0 || result.status.equals("RECONCILING")) return;
            String stage = result.status.equals("WAITING_FEEDBACK") || result.status.equals("REJECTED") ? "PRECHECK" : "COMPLETE";
            String recipient = taskRecords.findById(task.taskRecordId).orElseThrow().notificationUrl;
            String notificationId = UUID.randomUUID().toString();
            String payload;
            if (task.entryType.equals("XML")) payload = LegacyXml.notification(notificationId, task, stage, result);
            else {
                var notice = json.createObjectNode();
                notice.put("notificationId", notificationId); notice.put("taskRecordId", task.taskRecordId);
                notice.put("taskId", task.taskId); notice.put("stage", stage); notice.put("result", result.status);
                if (result.errorType != null) notice.put("errorType", result.errorType);
                if (result.errorDetail != null) notice.put("errorDetail", result.errorDetail);
                payload = notice.toString();
            }
            int inserted = db.insertNotification(notificationId, task.taskRecordId, stage, result.status,
                    task.entryType, recipient, payload, sha256(payload));
            if (inserted == 0 && !result.status.equals(notificationRecords.findByTaskIdAndStage(task.taskRecordId, stage).resultStatus))
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
