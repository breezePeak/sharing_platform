package com.siwei.endpoint;

import java.time.Instant;
import java.util.UUID;
import javax.persistence.*;

@Entity
@Table(name = "endpoint_operation")
public class EndpointOperation {
    @Id @Column(length = 36) public String id;
    @Column(name = "endpoint_id", nullable = false, length = 36) public String endpointId;
    @Column(nullable = false) public long revision;
    @Column(nullable = false, length = 30) public String action;
    @Column(name = "occurred_at", nullable = false) public Instant occurredAt;
    protected EndpointOperation() { }
    EndpointOperation(Endpoint endpoint, String action) {
        id = UUID.randomUUID().toString(); endpointId = endpoint.id;
        revision = endpoint.revision; this.action = action; occurredAt = Instant.now();
    }
}
