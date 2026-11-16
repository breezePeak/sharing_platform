package com.siwei.transfer;

import com.fasterxml.jackson.databind.*;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class NotificationDispatcher {
    private final JdbcTemplate db;
    private final ObjectMapper json;
    private final int maxAttempts;
    private final TransactionTemplate tx;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5))
            .followRedirects(HttpClient.Redirect.NEVER).build();

    public NotificationDispatcher(JdbcTemplate db, ObjectMapper json, TransferSettings settings,
                                  org.springframework.transaction.PlatformTransactionManager manager) {
        this.db = db; this.json = json;
        this.tx = new TransactionTemplate(manager);
        this.maxAttempts = Math.max(1, Math.min(20, settings.getNotificationAttempts()));
    }

    @Scheduled(fixedDelay = 1000)
    public void poll() {
        List<Map<String, Object>> due = db.queryForList("SELECT id,task_id,protocol,recipient_url,payload FROM transfer_notification WHERE (status IN ('PENDING','RETRY_WAIT') AND next_attempt_at<=now()) OR (status='SENDING' AND claimed_at<now()-interval '1 minute') ORDER BY next_attempt_at LIMIT 10");
        for (Map<String, Object> row : due) send(row);
    }

    private void send(Map<String, Object> row) {
        String id = (String) row.get("id"), taskId = (String) row.get("task_id");
        String claim = UUID.randomUUID().toString();
        int updated = db.update("UPDATE transfer_notification SET status='SENDING',claim_id=?,claimed_at=now(),attempts=attempts+1 WHERE id=? AND ((status IN ('PENDING','RETRY_WAIT') AND next_attempt_at<=now()) OR (status='SENDING' AND claimed_at<now()-interval '1 minute'))", claim, id);
        if (updated == 0) return;
        String error = null, ack = null;
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create((String) row.get("recipient_url")))
                    .timeout(Duration.ofSeconds(10)).header("Content-Type", row.get("protocol").equals("XML") ? "application/xml" : "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString((String) row.get("payload"))).build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) throw new IllegalStateException("HTTP " + response.statusCode());
            if (response.body().length() > 4096) throw new IllegalStateException("ACK_TOO_LARGE");
            if (row.get("protocol").equals("XML")) {
                if (!LegacyXml.ack(response.body(), id, taskId)) throw new IllegalStateException("ACK_MISMATCH");
            } else {
                JsonNode body = json.readTree(response.body());
                if (!body.path("accepted").isBoolean() || !body.path("accepted").asBoolean()
                        || !id.equals(body.path("notificationId").asText())
                        || !taskId.equals(body.path("taskRecordId").asText())) throw new IllegalStateException("ACK_MISMATCH");
            }
            ack = response.body();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); error = "INTERRUPTED";
        } catch (Exception e) {
            error = e.getClass().getSimpleName();
        }
        if (ack != null) {
            String acceptedAck = ack;
            tx.executeWithoutResult(status -> {
                int marked = db.update("UPDATE transfer_notification SET status='ACKED',ack=?,claim_id=NULL,claimed_at=NULL,last_error=NULL WHERE id=? AND claim_id=? AND status='SENDING'", acceptedAck, id, claim);
                if (marked == 1) db.update("UPDATE transfer_task SET status='QUEUED',phase='PRECHECK_ACKED',attempts=0,next_run_at=now(),version=version+1,updated_at=now() WHERE id=? AND status='WAITING_FEEDBACK' AND EXISTS (SELECT 1 FROM transfer_notification WHERE id=? AND stage='PRECHECK' AND result_status='WAITING_FEEDBACK')", taskId, id);
            });
        } else {
            int attempts = db.queryForObject("SELECT attempts FROM transfer_notification WHERE id=?", Integer.class, id);
            String nextStatus = attempts >= maxAttempts ? "NEEDS_ATTENTION" : "RETRY_WAIT";
            long delay = Math.min(300, 1L << Math.min(8, attempts));
            db.update("UPDATE transfer_notification SET status=?,next_attempt_at=?,last_error=?,claim_id=NULL,claimed_at=NULL WHERE id=? AND claim_id=? AND status='SENDING'",
                    nextStatus, java.sql.Timestamp.from(Instant.now().plusSeconds(delay)), error, id, claim);
        }
    }
}
