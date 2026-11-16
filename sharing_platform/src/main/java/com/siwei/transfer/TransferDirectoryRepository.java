package com.siwei.transfer;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TransferDirectoryRepository extends JpaRepository<TransferDirectoryRecord, TransferPathId> {
    List<TransferDirectoryRecord> findByTaskId(String taskId);
}
