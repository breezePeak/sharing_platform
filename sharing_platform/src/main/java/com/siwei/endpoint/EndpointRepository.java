package com.siwei.endpoint;

import javax.persistence.LockModeType;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import java.util.Optional;

public interface EndpointRepository extends JpaRepository<Endpoint, String> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select e from Endpoint e where e.id = :id")
    Optional<Endpoint> lockById(@Param("id") String id);
}
