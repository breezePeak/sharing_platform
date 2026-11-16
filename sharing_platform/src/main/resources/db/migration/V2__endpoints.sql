CREATE TABLE endpoint (
    id varchar(36) PRIMARY KEY,
    revision bigint NOT NULL,
    enabled boolean NOT NULL,
    lock_version bigint NOT NULL DEFAULT 0
);
CREATE TABLE endpoint_revision (
    id varchar(36) PRIMARY KEY,
    endpoint_id varchar(36) NOT NULL REFERENCES endpoint(id),
    revision bigint NOT NULL,
    name varchar(100) NOT NULL,
    protocol varchar(10) NOT NULL CHECK (protocol IN ('FTP','SFTP')),
    host varchar(253) NOT NULL,
    port integer NOT NULL CHECK (port BETWEEN 1 AND 65535),
    username varchar(100) NOT NULL,
    auth varchar(10) NOT NULL CHECK (auth IN ('PASSWORD','KEY')),
    secret text,
    key_file varchar(100),
    key_digest varchar(64),
    fingerprint varchar(100),
    read_root varchar(1000),
    write_root varchar(1000),
    read_test_file varchar(500),
    probe_directory varchar(500),
    local_source_ref varchar(100),
    local_target_ref varchar(100),
    timeout_seconds integer NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 30),
    concurrency integer NOT NULL CHECK (concurrency BETWEEN 1 AND 8),
    passive boolean NOT NULL,
    isolated_ftp_root boolean NOT NULL,
    created_at timestamp with time zone NOT NULL,
    UNIQUE(endpoint_id, revision)
);
CREATE TABLE endpoint_probe (
    id varchar(36) PRIMARY KEY,
    endpoint_id varchar(36) NOT NULL REFERENCES endpoint(id),
    revision_id varchar(36) NOT NULL REFERENCES endpoint_revision(id),
    started_at timestamp with time zone NOT NULL,
    finished_at timestamp with time zone,
    status varchar(30) NOT NULL,
    can_read boolean NOT NULL DEFAULT false,
    can_write boolean NOT NULL DEFAULT false,
    verified boolean NOT NULL DEFAULT false,
    can_rename boolean NOT NULL DEFAULT false,
    details text NOT NULL,
    residuals text NOT NULL,
    capacity varchar(30) NOT NULL DEFAULT 'UNKNOWN'
);
CREATE INDEX endpoint_probe_latest ON endpoint_probe(endpoint_id, started_at DESC);
CREATE TABLE endpoint_operation (
    id varchar(36) PRIMARY KEY,
    endpoint_id varchar(36) NOT NULL REFERENCES endpoint(id),
    revision bigint NOT NULL,
    action varchar(30) NOT NULL,
    occurred_at timestamp with time zone NOT NULL
);
