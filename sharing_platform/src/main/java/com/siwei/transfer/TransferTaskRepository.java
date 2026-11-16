package com.siwei.transfer;

import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import javax.persistence.LockModeType;

public interface TransferTaskRepository extends JpaRepository<TransferTaskRecord, String> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select t from TransferTaskRecord t where t.id = :id")
    Optional<TransferTaskRecord> lockById(@Param("id") String id);
    Optional<TransferTaskRecord> findByCallerIdAndExternalTaskId(String callerId, String externalTaskId);
    Optional<TransferTaskRecord> findByIdAndCallerId(String id, String callerId);
}
