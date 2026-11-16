package com.siwei.transfer;

import java.time.Instant;
import javax.persistence.*;

@Entity
@Table(name = "transfer_notification")
public class TransferNotificationRecord {
    @Id public String id;
    @Column(name = "task_id") public String taskId;
    public String stage;
    @Column(name = "result_status") public String resultStatus;
    public String protocol;
    @Column(name = "recipient_url") public String recipientUrl;
    public String payload;
    @Column(name = "payload_sha256") public String payloadSha256;
    public int attempts;
    @Column(name = "next_attempt_at") public Instant nextAttemptAt;
    public String status;
    @Column(name = "claim_id") public String claimId;
    @Column(name = "claimed_at") public Instant claimedAt;
    public String ack;
    @Column(name = "last_error") public String lastError;
    @Column(name = "created_at") public Instant createdAt;
}
