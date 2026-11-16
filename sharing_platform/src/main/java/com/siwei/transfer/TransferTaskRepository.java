package com.siwei.transfer;

import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TransferTaskRepository extends JpaRepository<TransferTaskRecord, String> {
    Optional<TransferTaskRecord> findByCallerIdAndExternalTaskId(String callerId, String externalTaskId);
    Optional<TransferTaskRecord> findByIdAndCallerId(String id, String callerId);
}
