package com.siwei.endpoint;

import java.time.Instant;
import javax.persistence.*;

/** Immutable configuration/credential snapshot; future tasks reference this ID. */
@Entity
@Table(name = "endpoint_revision")
@org.hibernate.annotations.Immutable
public class EndpointRevision {
    public enum Protocol { FTP, SFTP }
    public enum Auth { PASSWORD, KEY }
    @Id @Column(length = 36) public String id;
    @Column(name = "endpoint_id", nullable = false, length = 36) public String endpointId;
    @Column(nullable = false) public long revision;
    @Column(nullable = false, length = 100) public String name;
    @Enumerated(EnumType.STRING) @Column(nullable = false, length = 10) public Protocol protocol;
    @Column(nullable = false, length = 253) public String host;
    @Column(nullable = false) public int port;
    @Column(nullable = false, length = 100) public String username;
    @Enumerated(EnumType.STRING) @Column(nullable = false, length = 10) public Auth auth;
    @Column(columnDefinition = "text") String secret;
    @Column(name = "key_file", length = 100) public String keyFile;
    @Column(name = "key_digest", length = 64) public String keyDigest;
    @Column(length = 100) public String fingerprint;
    @Column(name = "read_root", length = 1000) public String readRoot;
    @Column(name = "write_root", length = 1000) public String writeRoot;
    @Column(name = "read_test_file", length = 500) public String readTestFile;
    @Column(name = "probe_directory", length = 500) public String probeDirectory;
    @Column(name = "local_source_ref", length = 100) public String localSourceRef;
    @Column(name = "local_target_ref", length = 100) public String localTargetRef;
    @Column(name = "timeout_seconds", nullable = false) public int timeoutSeconds;
    @Column(nullable = false) public int concurrency;
    @Column(nullable = false) public boolean passive;
    @Column(name = "isolated_ftp_root", nullable = false) public boolean isolatedFtpRoot;
    @Column(name = "created_at", nullable = false) public Instant createdAt;

    public boolean hasSecret() { return secret != null; }
}
