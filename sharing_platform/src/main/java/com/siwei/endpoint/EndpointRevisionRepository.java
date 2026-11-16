package com.siwei.endpoint;

import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EndpointRevisionRepository extends JpaRepository<EndpointRevision, String> {
    Optional<EndpointRevision> findByEndpointIdAndRevision(String endpointId, long revision);
}
