package com.siwei.endpoint;

import javax.persistence.*;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/** Endpoint row lock serializes task claims and probe creation across all revisions. */
@Repository
@Transactional
public class EndpointCapacity {
    @PersistenceContext private EntityManager em;
    public boolean available(String endpointId, int limit, String recoveringTask) {
        em.find(Endpoint.class, endpointId, LockModeType.PESSIMISTIC_WRITE);
        Object[] used = (Object[]) em.createNativeQuery(
                "SELECT count(*),coalesce(min(concurrency),8) FROM ("
                + "SELECT r.concurrency FROM transfer_task t JOIN endpoint_revision r ON r.id=t.endpoint_revision_id "
                + "WHERE t.endpoint_id=:endpoint AND t.status='RUNNING' AND t.id<>:task "
                + "UNION ALL SELECT r.concurrency FROM endpoint_probe p JOIN endpoint_revision r ON r.id=p.revision_id "
                + "WHERE p.endpoint_id=:endpoint AND p.status='RUNNING') sessions")
                .setParameter("endpoint", endpointId).setParameter("task", recoveringTask == null ? "" : recoveringTask).getSingleResult();
        return ((Number) used[0]).longValue() < Math.min(limit, ((Number) used[1]).intValue());
    }
}
