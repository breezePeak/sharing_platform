package com.siwei.endpoint;

import java.time.Instant;
import javax.persistence.*;

@Entity
@Table(name = "endpoint_probe")
public class EndpointProbe {
    @Id @Column(length = 36) public String id;
    @Column(name = "endpoint_id", nullable = false, length = 36) public String endpointId;
    @Column(name = "revision_id", nullable = false, length = 36) public String revisionId;
    @Column(name = "started_at", nullable = false) public Instant startedAt;
    @Column(name = "finished_at") public Instant finishedAt;
    @Column(nullable = false, length = 30) public String status;
    @Column(name = "can_read", nullable = false) public boolean canRead;
    @Column(name = "can_write", nullable = false) public boolean canWrite;
    @Column(nullable = false) public boolean verified;
    @Column(name = "can_rename", nullable = false) public boolean canRename;
    @Column(nullable = false, columnDefinition = "text") public String details = "";
    @Column(nullable = false, columnDefinition = "text") public String residuals = "";
    @Column(nullable = false, length = 30) public String capacity = "UNKNOWN";
    public boolean isStrictPush() { return "COMPLETED".equals(status) && canWrite && verified && canRename && residuals.isEmpty(); }
}
