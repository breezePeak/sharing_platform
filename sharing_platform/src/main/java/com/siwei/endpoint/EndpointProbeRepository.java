package com.siwei.endpoint;

import java.util.*;
import javax.persistence.LockModeType;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;

public interface EndpointProbeRepository extends JpaRepository<EndpointProbe, String> {
    Optional<EndpointProbe> findFirstByEndpointIdOrderByStartedAtDesc(String endpointId);
    long countByEndpointIdAndStatus(String endpointId, String status);
    List<EndpointProbe> findByStatus(String status);
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select p from EndpointProbe p where p.id = :id")
    Optional<EndpointProbe> lockById(@Param("id") String id);
}
