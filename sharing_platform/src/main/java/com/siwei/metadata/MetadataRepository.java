package com.siwei.metadata;

import org.springframework.data.jpa.repository.JpaRepository;

public interface MetadataRepository extends JpaRepository<PlatformMetadata, String> {
}
