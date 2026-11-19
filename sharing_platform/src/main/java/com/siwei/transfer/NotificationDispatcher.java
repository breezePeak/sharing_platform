package com.siwei.transfer;

import com.fasterxml.jackson.databind.*;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
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
@org.springframework.context.annotation.DependsOn("startupRecovery")
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name = "platform.recovery.dispatch-enabled", matchIfMissing = true)
public class NotificationDispatcher {
    private static final Logger log = LoggerFactory.getLogger(NotificationDispatcher.class);
    private final TransferJpa db;
    private final TransferNotificationRepository notificationRecords;
    private final ObjectMapper json;
    private final int maxAttempts;
    private final TransactionTemplate tx;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5))
            .followRedirects(HttpClient.Redirect.NEVER).build();
    private final ExecutorService readers = Executors.newCachedThreadPool(r -> {
        Thread thread = new Thread(r, "notification-ack"); thread.setDaemon(true); return thread;
    });

    public NotificationDispatcher(TransferJpa db, TransferNotificationRepository notificationRecords,
                                  ObjectMapper json, TransferSettings settings,
                                  org.springframework.transaction.PlatformTransactionManager manager) {
        this.db = db; this.notificationRecords = notificationRecords; this.json = json;
        this.tx = new TransactionTemplate(manager);
        this.maxAttempts = Math.max(1, Math.min(20, settings.getNotificationAttempts()));
    }

    @Scheduled(fixedDelay = 1000)
    public void poll() {
        List<Map<String, Object>> due = db.dueNotifications();
        for (Map<String, Object> row : due) send(row);
    }

    private void send(Map<String, Object> row) {
        String id = (String) row.get("id"), taskId = (String) row.get("task_id");
        String claim = UUID.randomUUID().toString();
        int updated = db.claimNotification(id, claim);
        if (updated == 0) return;
        String error = null, ack = null;
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create((String) row.get("recipient_url")))
                    .timeout(Duration.ofSeconds(10)).header("Content-Type", row.get("protocol").equals("XML") ? "application/xml" : "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString((String) row.get("payload"))).build();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            CompletableFuture<HttpResponse<InputStream>> pending = client.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream());
            HttpResponse<InputStream> response;
            try { response = pending.get(remaining(deadline), TimeUnit.NANOSECONDS); }
            catch (Exception e) { pending.cancel(true); throw e; }
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                response.body().close();
                throw new IllegalStateException("HTTP " + response.statusCode());
            }
            String body;
            try (InputStream stream = response.body()) {
                Future<byte[]> reading = readers.submit(() -> stream.readNBytes(4097));
                try {
                    byte[] bytes = reading.get(remaining(deadline), TimeUnit.NANOSECONDS);
                    if (bytes.length > 4096) throw new IllegalStateException("ACK_TOO_LARGE");
                    body = new String(bytes, StandardCharsets.UTF_8);
                } catch (Exception e) { reading.cancel(true); pending.cancel(true); throw e; }
            }
            if (row.get("protocol").equals("XML")) {
                if (!LegacyXml.ack(body, id, taskId)) throw new IllegalStateException("ACK_MISMATCH");
            } else {
                JsonNode parsed = json.readTree(body);
                if (!parsed.path("accepted").isBoolean() || !parsed.path("accepted").asBoolean()
                        || !id.equals(parsed.path("notificationId").asText())
                        || !taskId.equals(parsed.path("taskRecordId").asText())) throw new IllegalStateException("ACK_MISMATCH");
            }
            ack = body;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); error = "INTERRUPTED";
        } catch (Exception e) {
            error = e.getClass().getSimpleName();
        }
        if (ack != null) {
            String acceptedAck = ack;
            try {
                tx.executeWithoutResult(status -> {
                    int marked = db.ackNotification(id, claim, acceptedAck);
                    if (marked == 1) db.resumeAfterPrecheck(taskId, id);
                });
            } catch (RuntimeException persistenceFailure) {
                log.warn("notification ACK persistence failed id={} type={}", id, persistenceFailure.getClass().getSimpleName());
            }
        } else {
            int attempts = notificationRecords.findById(id).orElseThrow().attempts;
            String nextStatus = attempts >= maxAttempts ? "NEEDS_ATTENTION" : "RETRY_WAIT";
            long delay = Math.min(300, 1L << Math.min(8, attempts));
            db.retryNotification(id, claim, nextStatus, java.sql.Timestamp.from(Instant.now().plusSeconds(delay)), error);
        }
    }
    private static long remaining(long deadline) throws TimeoutException {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) throw new TimeoutException("ACK_DEADLINE");
        return nanos;
    }
    @PreDestroy public void close() { readers.shutdownNow(); }
}
