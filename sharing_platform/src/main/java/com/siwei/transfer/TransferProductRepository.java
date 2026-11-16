package com.siwei.transfer;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TransferProductRepository extends JpaRepository<TransferProductRecord, TransferProductRecord.Key> {
    List<TransferProductRecord> findByTaskIdOrderByPosition(String taskId);
}
