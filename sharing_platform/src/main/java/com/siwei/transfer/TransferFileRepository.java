package com.siwei.transfer;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TransferFileRepository extends JpaRepository<TransferFileRecord, TransferPathId> {
    List<TransferFileRecord> findByTaskId(String taskId);
    long countByTaskId(String taskId);
}
