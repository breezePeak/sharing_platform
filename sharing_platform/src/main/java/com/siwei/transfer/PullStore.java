package com.siwei.transfer;

import java.util.*;
import javax.persistence.*;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/** Pull records share the task/outbox JPA transaction manager. */
@Repository
@Transactional
public class PullStore {
    @PersistenceContext private EntityManager em;

    public int update(String sql, Object... args) {
        em.flush();
        int count = bind(em.createNativeQuery(sql), args).executeUpdate();
        em.clear();
        return count;
    }
    public List<Map<String, Object>> rows(String sql, Object... args) {
        @SuppressWarnings("unchecked") List<Tuple> tuples = bind(em.createNativeQuery(sql, Tuple.class), args).getResultList();
        List<Map<String, Object>> result = new ArrayList<>();
        for (Tuple tuple : tuples) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (TupleElement<?> column : tuple.getElements()) row.put(column.getAlias(), tuple.get(column));
            result.add(row);
        }
        return result;
    }
    public Map<String, Object> one(String sql, Object... args) { return rows(sql, args).get(0); }
    private static Query bind(Query query, Object[] args) {
        for (int i = 0; i < args.length; i++) query.setParameter(i + 1, args[i]);
        return query;
    }

    public void snapshot(String task, String sourceId, String config) {
        update("INSERT INTO pull_source_revision(source_id,config_sha256,config) VALUES (?,?,?) ON CONFLICT DO NOTHING",
                sourceId, TaskWorker.sha256(config), config);
        update("UPDATE transfer_task SET pull_config=? WHERE id=?", config, task);
    }

    public List<Map<String, Object>> deliveries(String task) {
        return rows("SELECT u.source_path,u.status AS unit_status,u.error_type AS unit_error,t.id AS transfer_id,t.source_version,t.version_basis,t.version_strength,t.status,t.acceptance_status,t.error_type,t.acceptance_reason,t.acceptance_category,t.acceptance_reminded_at,t.data_location,t.published_at,t.reconcile_attempts FROM pull_task_unit u LEFT JOIN pull_transfer t ON t.id=u.transfer_id WHERE u.task_id=? ORDER BY u.source_path", task);
    }
    public List<Map<String, Object>> notifications(String task) {
        return rows("SELECT n.id,n.stage,n.status,n.attempts,n.last_error,n.transfer_id FROM transfer_notification n WHERE n.task_id=? OR n.transfer_id IN (SELECT transfer_id FROM pull_task_transfer WHERE task_id=?) ORDER BY n.created_at", task, task);
    }
    public List<Map<String, Object>> files(String task, int size, long offset) {
        return rows("SELECT f.transfer_id,f.relative_path,f.size_bytes,f.expected_hash,f.actual_hash,f.downloaded_bytes,f.verified FROM pull_file f JOIN pull_task_unit u ON u.transfer_id=f.transfer_id WHERE u.task_id=? ORDER BY f.transfer_id,f.relative_path LIMIT ? OFFSET ?", task, size, offset);
    }
    public Map<String, Object> progress(String task) {
        return one("SELECT count(*) AS total_files,coalesce(sum(f.size_bytes),0) AS total_bytes,coalesce(sum(f.downloaded_bytes),0) AS downloaded_bytes,coalesce(sum(f.size_bytes) FILTER (WHERE f.verified),0) AS verified_bytes,count(*) FILTER (WHERE f.verified) AS verified_files FROM pull_file f JOIN pull_task_unit u ON u.transfer_id=f.transfer_id WHERE u.task_id=?", task);
    }

    public void fileProgress(String transfer, String path, String claim, long downloaded, String hash, boolean verified) {
        Map<String, Object> delivery = one("SELECT claim_id FROM pull_transfer WHERE id=? FOR UPDATE", transfer);
        if (claim == null || !claim.equals(delivery.get("claim_id"))) throw new IllegalStateException("DELIVERY_CLAIM_LOST");
        if (update("UPDATE pull_file SET downloaded_bytes=?,actual_hash=?,verified=? WHERE transfer_id=? AND relative_path=?", downloaded, hash, verified, transfer, path) != 1)
            throw new IllegalStateException("FILE_NOT_FOUND");
        update("UPDATE pull_transfer SET current_file=? WHERE id=?", path, transfer);
    }

    // Recompute under the task row lock so a worker's late result cannot undo a receipt.
    public void aggregate(String task, String claim, int delaySeconds) {
        Map<String, Object> record = one("SELECT status,claim_id,pull_scan_complete FROM transfer_task WHERE id=? FOR UPDATE", task);
        if (claim != null && !claim.equals(record.get("claim_id"))) return;
        List<Map<String, Object>> units = deliveries(task);
        boolean runnable = !(Boolean) record.get("pull_scan_complete"), blockedPublication = false;
        boolean pending = false, failed = false, rejected = false;
        for (Map<String, Object> u : units) {
            if ("REJECTED".equals(u.get("acceptance_status"))) rejected = true;
            if (u.get("transfer_id") == null) {
                if ("FAILED".equals(u.get("unit_status"))) failed = true; else runnable = true;
            } else if ("FAILED".equals(u.get("status")) || "REJECTED".equals(u.get("acceptance_status"))) failed = true;
            else if ("PUBLISHING".equals(u.get("status")) && ((Number) u.get("reconcile_attempts")).intValue() >= TransferJpa.RECONCILE_LIMIT)
                blockedPublication = true;
            else if (!"PUBLISHED".equals(u.get("status"))) runnable = true;
            else if (!"ACCEPTED".equals(u.get("acceptance_status"))) pending = true;
        }
        boolean waiting = runnable || blockedPublication;
        String state = waiting ? "WAITING_SOURCE" : pending ? "WAITING_ACCEPTANCE" : failed ? "FAILED" : units.isEmpty() ? "NO_NEW_DATA" : "SUCCEEDED";
        String acceptance = pending ? "PENDING" : waiting ? "NOT_STARTED" : failed ? rejected ? "REJECTED" : "NOT_STARTED" : units.isEmpty() ? "NOT_STARTED" : "ACCEPTED";
        String error = failed ? "UNIT_FAILED" : null;
        if (claim == null && record.get("claim_id") != null) {
            update("UPDATE transfer_task SET acceptance_status=?,updated_at=now() WHERE id=?", acceptance, task);
            return;
        }
        update("UPDATE transfer_task SET status=?,phase=?,acceptance_status=?,error_type=?,error_detail=NULL,"
                        + "next_run_at=now()+(? * interval '1 second'),attempts=0,claim_id=NULL,claimed_at=NULL,updated_at=now(),version=version+1,"
                        + "read_only_recheck=read_only_recheck AND NOT ?,reconcile_attempts=? WHERE id=?",
                state, state, acceptance, error, delaySeconds, claim != null, blockedPublication && !runnable ? TransferJpa.RECONCILE_LIMIT : 0, task);
    }
}
