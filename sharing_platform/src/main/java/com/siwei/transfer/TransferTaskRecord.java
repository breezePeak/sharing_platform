package com.siwei.transfer;

import java.time.Instant;
import javax.persistence.*;

@Entity
@Table(name = "transfer_task")
public class TransferTaskRecord {
    @Id public String id;
    @Column(name = "caller_id") public String callerId;
    @Column(name = "external_task_id") public String externalTaskId;
    @Column(name = "entry_type") public String entryType;
    public String direction;
    @Column(name = "endpoint_id") public String endpointId;
    @Column(name = "endpoint_revision_id") public String endpointRevisionId;
    @Column(name = "notification_url") public String notificationUrl;
    @Column(name = "canonical_request") public String canonicalRequest;
    @Column(name = "raw_request") public String rawRequest;
    @Column(name = "push_manifest_sha256") public String pushManifestSha256;
    @Column(name = "push_source_root") public String pushSourceRoot;
    @Column(name = "push_target_root") public String pushTargetRoot;
    @Column(name = "push_publish_intent") public boolean pushPublishIntent;
    @Column(name = "push_manifest_intent") public boolean pushManifestIntent;
    @Column(name = "push_manifest") public String pushManifest;
    @Column(name = "push_route_snapshot") public String pushRouteSnapshot;
    @Column(name = "pull_config") public String pullConfig;
    @Column(name = "pull_scan_complete") public boolean pullScanComplete;
    @Column(name = "trigger_type") public String triggerType = "API";
    @Column(name = "read_only_recheck") public boolean readOnlyRecheck;
    @Column(name = "current_file") public String currentFile;
    @Column(name = "listener_id") public String listenerId;
    @Column(name = "listener_revision") public Integer listenerRevision;
    public String status, phase;
    @Column(name = "acceptance_status") public String acceptanceStatus;
    @Column(name = "error_type") public String errorType;
    @Column(name = "error_detail") public String errorDetail;
    @Column(name = "next_run_at") public Instant nextRunAt;
    @Column(name = "claim_id") public String claimId;
    @Column(name = "claimed_at") public Instant claimedAt;
    public int attempts;
    public long version;
    @Column(name = "created_at") public Instant createdAt;
    @Column(name = "updated_at") public Instant updatedAt;
}
