package com.siwei.transfer;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TransferNotificationRepository extends JpaRepository<TransferNotificationRecord, String> {
    List<TransferNotificationRecord> findByTaskIdOrderByCreatedAt(String taskId);
    TransferNotificationRecord findByTaskIdAndStage(String taskId, String stage);
}
