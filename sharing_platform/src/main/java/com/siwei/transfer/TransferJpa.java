package com.siwei.transfer;

import java.util.*;
import javax.persistence.*;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/** Atomic native statements for the transfer tables, executed through JPA transactions. */
@Repository
@Transactional
public class TransferJpa {
    @PersistenceContext private EntityManager entityManager;
    private final com.siwei.endpoint.EndpointCapacity capacity;
    private final TransferFileRepository files;
    public TransferJpa(com.siwei.endpoint.EndpointCapacity capacity, TransferFileRepository files) { this.capacity = capacity; this.files = files; }

    private int update(String sql, Object... values) {
        entityManager.flush();
        int count = bind(entityManager.createNativeQuery(sql), values).executeUpdate();
        entityManager.clear();
        return count;
    }

    private List<Map<String, Object>> queryForList(String sql, Object... values) {
        List<Map<String, Object>> rows = new ArrayList<>();
        @SuppressWarnings("unchecked") List<Tuple> tuples = bind(entityManager.createNativeQuery(sql, Tuple.class), values).getResultList();
        for (Tuple tuple : tuples) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (TupleElement<?> column : tuple.getElements()) row.put(column.getAlias(), tuple.get(column));
            rows.add(row);
        }
        return rows;
    }

    private static Query bind(Query query, Object[] values) {
        for (int i = 0; i < values.length; i++) query.setParameter(i + 1, values[i]);
        return query;
    }

    public int insertTask(String id, String caller, String taskId, String entry, String direction,
                          String endpointId, String revisionId, String notificationUrl, String canonical,
                          String raw, String sourceRoot, java.sql.Timestamp now) {
        return update("INSERT INTO transfer_task (id,caller_id,external_task_id,entry_type,direction,endpoint_id,endpoint_revision_id,notification_url,canonical_request,raw_request,push_source_root,status,phase,next_run_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,'QUEUED','ACCEPTED',?,?,?) ON CONFLICT (caller_id,external_task_id) DO NOTHING",
                id, caller, taskId, entry, direction, endpointId, revisionId, notificationUrl, canonical, raw, sourceRoot, now, now, now);
    }

    public List<Map<String, Object>> dueTasks(int limit, int offset) {
        return queryForList("SELECT id,direction FROM (SELECT id,direction,next_run_at,"
                + "row_number() OVER (PARTITION BY endpoint_id ORDER BY next_run_at,id) AS priority FROM transfer_task "
                + "WHERE (status IN ('QUEUED','RECONCILING','WAITING_SOURCE','WAITING_ACCEPTANCE') AND next_run_at<=now()) "
                + "OR (status='RUNNING' AND claimed_at<now()-interval '5 minutes')) due "
                + "ORDER BY priority,next_run_at,id LIMIT ? OFFSET ?", limit, offset);
    }
    public int claimTask(String id, String claim) {
        List<Map<String, Object>> candidates = queryForList("SELECT t.endpoint_id,r.concurrency FROM transfer_task t JOIN endpoint_revision r ON r.id=t.endpoint_revision_id WHERE t.id=?", id);
        if (candidates.isEmpty()) return 0;
        Map<String, Object> task = candidates.get(0);
        if (!capacity.available((String) task.get("endpoint_id"), ((Number) task.get("concurrency")).intValue(), id)) return 0;
        return update("UPDATE transfer_task SET status='RUNNING',claim_id=?,claimed_at=now(),attempts=attempts+1,version=version+1,updated_at=now() WHERE id=? AND ((status IN ('QUEUED','RECONCILING','WAITING_SOURCE','WAITING_ACCEPTANCE') AND next_run_at<=now()) OR (status='RUNNING' AND claimed_at<now()-interval '5 minutes'))", claim, id);
    }
    public int releaseTaskClaim(String id, String claim) {
        return update("UPDATE transfer_task SET status='QUEUED',claim_id=NULL,claimed_at=NULL,attempts=attempts-1,next_run_at=now(),updated_at=now() WHERE id=? AND claim_id=? AND status='RUNNING'", id, claim);
    }
    public int retryTask(String id, String claim, String error, java.sql.Timestamp next) {
        return update("UPDATE transfer_task SET status='QUEUED',phase='RETRY_WAIT',error_type='NETWORK_TEMP',error_detail=?,next_run_at=?,claim_id=NULL,claimed_at=NULL,updated_at=now() WHERE id=? AND claim_id=? AND status='RUNNING'",
                error, next, id, claim);
    }
    public int heartbeatTask(String id, String claim) {
        update("UPDATE pull_transfer SET claimed_at=now() WHERE claim_id=? AND EXISTS (SELECT 1 FROM transfer_task WHERE id=? AND claim_id=? AND status='RUNNING')", claim, id, claim);
        return update("UPDATE transfer_task SET claimed_at=now() WHERE id=? AND claim_id=? AND status='RUNNING'", id, claim);
    }
    public int finishTask(String id, String claim, TaskProcessor.Result result) {
        return update("UPDATE transfer_task SET status=?,phase=CASE WHEN ?='COMPLETE' THEN 'NOTIFYING' ELSE ? END,error_type=?,error_detail=?,next_run_at=CASE WHEN ?='RECONCILING' THEN now()+interval '30 seconds' ELSE next_run_at END,claim_id=NULL,claimed_at=NULL,version=version+1,updated_at=now(),read_only_recheck=false,current_file=NULL WHERE id=? AND claim_id=? AND status='RUNNING'",
                result.status, result.phase, result.phase, result.errorType, result.errorDetail, result.status, id, claim);
    }
    public int insertNotification(String id, String taskId, String stage, String result, String protocol,
                                  String recipient, String payload, String hash) {
        return update("INSERT INTO transfer_notification (id,task_id,stage,result_status,protocol,recipient_url,payload,payload_sha256,next_attempt_at,status,created_at) VALUES (?,?,?,?,?,?,?,?,now(),'PENDING',now()) ON CONFLICT (task_id,stage) DO NOTHING",
                id, taskId, stage, result, protocol, recipient, payload, hash);
    }
    public List<Map<String, Object>> dueNotifications() {
        return queryForList("SELECT id,task_id,protocol,recipient_url,payload FROM transfer_notification WHERE (status IN ('PENDING','RETRY_WAIT') AND next_attempt_at<=now()) OR (status='SENDING' AND claimed_at<now()-interval '1 minute') ORDER BY next_attempt_at LIMIT 10");
    }
    public int claimNotification(String id, String claim) {
        return update("UPDATE transfer_notification SET status='SENDING',claim_id=?,claimed_at=now(),attempts=attempts+1 WHERE id=? AND ((status IN ('PENDING','RETRY_WAIT') AND next_attempt_at<=now()) OR (status='SENDING' AND claimed_at<now()-interval '1 minute'))", claim, id);
    }
    public int ackNotification(String id, String claim, String ack) {
        int count = update("UPDATE transfer_notification SET status='ACKED',ack=?,claim_id=NULL,claimed_at=NULL,last_error=NULL WHERE id=? AND claim_id=? AND status='SENDING'", ack, id, claim);
        if (count == 1) update("UPDATE transfer_task SET phase='COMPLETE',updated_at=now() WHERE phase='NOTIFYING' AND id IN (SELECT task_id FROM transfer_notification WHERE id=? AND stage='COMPLETE' AND status='ACKED')", id);
        return count;
    }
    public int resumeAfterPrecheck(String taskId, String notificationId) {
        return update("UPDATE transfer_task SET status='QUEUED',phase='PRECHECK_ACKED',attempts=0,next_run_at=now(),version=version+1,updated_at=now() WHERE id=? AND status='WAITING_FEEDBACK' AND EXISTS (SELECT 1 FROM transfer_notification WHERE id=? AND stage='PRECHECK' AND result_status='WAITING_FEEDBACK')", taskId, notificationId);
    }
    public int retryNotification(String id, String claim, String status, java.sql.Timestamp next, String error) {
        return update("UPDATE transfer_notification SET status=?,next_attempt_at=?,last_error=?,claim_id=NULL,claimed_at=NULL WHERE id=? AND claim_id=? AND status='SENDING'",
                status, next, error, id, claim);
    }
    public Map<String, Object> pushProgress(String taskId) {
        return queryForList("SELECT count(*) AS total_files,coalesce(sum(size_bytes),0) AS total_bytes,coalesce(sum(uploaded_bytes),0) AS uploaded_bytes,coalesce(sum(upload_traffic_bytes),0) AS upload_traffic_bytes,coalesce(sum(readback_bytes),0) AS readback_bytes,coalesce(sum(greatest(upload_traffic_bytes-uploaded_bytes,0)),0) AS retransmitted_bytes,count(*) FILTER (WHERE verified) AS verified_files,coalesce(sum(size_bytes) FILTER (WHERE verified),0) AS verified_bytes FROM transfer_file WHERE task_id=?", taskId).get(0);
    }
    public long fileCount(String taskId) {
        return files.countByTaskId(taskId);
    }
    public List<Map<String, Object>> filePage(String taskId, int size, long offset) {
        List<Map<String, Object>> rows = new ArrayList<>();
        files.findByTaskId(taskId).stream().sorted(TransferFileRecord.ORDER).skip(offset).limit(size).forEach(file ->
                rows.add(Map.of("product_id", file.product, "relative_path", file.path, "size_bytes", file.size, "sha256", file.sha, "verified", file.verified)));
        return rows;
    }
}
