package com.siwei.endpoint;

import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

@Service
public class ProbeStore {
    private final EndpointRepository endpoints;
    private final EndpointRevisionRepository revisions;
    private final EndpointProbeRepository probes;
    private final EndpointOperationRepository operations;
    private final EndpointCapacity capacity;
    public ProbeStore(EndpointRepository endpoints, EndpointRevisionRepository revisions,
                      EndpointProbeRepository probes, EndpointOperationRepository operations, EndpointCapacity capacity) {
        this.endpoints = endpoints; this.revisions = revisions; this.probes = probes; this.operations = operations;
        this.capacity = capacity;
    }

    @Transactional
    public EndpointProbe create(String endpointId, long revision) {
        Endpoint e = endpoints.lockById(endpointId).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
        if (!e.enabled) throw new IllegalArgumentException("停用连接不能开始新检测");
        if (e.revision != revision) throw new ResponseStatusException(HttpStatus.CONFLICT);
        EndpointRevision r = revisions.findByEndpointIdAndRevision(endpointId, revision).orElseThrow();
        if (!capacity.available(endpointId, r.concurrency, null))
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS);
        EndpointProbe p = new EndpointProbe();
        p.id = UUID.randomUUID().toString(); p.endpointId = endpointId; p.revisionId = r.id;
        p.startedAt = Instant.now(); p.status = "RUNNING";
        operations.save(new EndpointOperation(e, "PROBE"));
        return probes.saveAndFlush(p);
    }

    @Transactional
    public boolean progress(String id, String details, String residuals) {
        EndpointProbe p = probes.lockById(id).orElseThrow();
        if (!p.status.equals("RUNNING")) return false;
        p.details = details; p.residuals = residuals;
        return true;
    }

    @Transactional
    public void finish(EndpointProbe result) {
        EndpointProbe p = probes.lockById(result.id).orElseThrow();
        if (!p.status.equals("RUNNING")) return;
        p.status = result.status; p.finishedAt = Instant.now(); p.details = result.details; p.residuals = result.residuals;
        p.canRead = result.canRead; p.canWrite = result.canWrite; p.verified = result.verified;
        p.canRename = result.canRename; p.capacity = result.capacity;
    }

    @Transactional
    public void stop(String id, String status) {
        EndpointProbe p = probes.lockById(id).orElseThrow();
        if (!p.status.equals("RUNNING")) return;
        p.status = status; p.finishedAt = Instant.now(); p.details += "\n" + status;
    }

    @Transactional
    public void recoverInterrupted() {
        for (EndpointProbe p : probes.findByStatus("RUNNING")) {
            p.status = "INTERRUPTED"; p.finishedAt = Instant.now(); p.details += "\nRESTART_REQUIRES_REVIEW";
        }
    }
}
