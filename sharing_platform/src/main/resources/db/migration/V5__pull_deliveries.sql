ALTER TABLE transfer_task ADD COLUMN pull_config text;
ALTER TABLE transfer_task ADD COLUMN pull_scan_complete boolean NOT NULL DEFAULT false;
ALTER TABLE transfer_notification ALTER COLUMN stage TYPE varchar(80);
ALTER TABLE transfer_notification ADD COLUMN transfer_id varchar(36);

CREATE TABLE pull_source_revision (
    source_id varchar(100) NOT NULL,
    config_sha256 varchar(64) NOT NULL,
    config text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, config_sha256)
);
CREATE TABLE pull_candidate (
    candidate_key varchar(64) PRIMARY KEY,
    source_id varchar(100) NOT NULL,
    source_path varchar(1000) NOT NULL,
    observation text NOT NULL,
    stable_count integer NOT NULL,
    observed_at timestamptz NOT NULL,
    last_error varchar(100)
);
CREATE TABLE pull_delivery_key (
    delivery_key varchar(64) PRIMARY KEY,
    current_transfer_id varchar(36)
);
CREATE TABLE pull_transfer (
    id varchar(36) PRIMARY KEY,
    delivery_key varchar(64) NOT NULL REFERENCES pull_delivery_key(delivery_key),
    previous_transfer_id varchar(36) REFERENCES pull_transfer(id),
    owner_task_id varchar(36) NOT NULL REFERENCES transfer_task(id),
    endpoint_id varchar(36) NOT NULL REFERENCES endpoint(id),
    source_path varchar(1000) NOT NULL,
    source_version varchar(64) NOT NULL,
    version_basis varchar(40) NOT NULL,
    version_strength varchar(10) NOT NULL,
    observation text NOT NULL,
    config text NOT NULL,
    status varchar(30) NOT NULL DEFAULT 'DOWNLOADING',
    acceptance_status varchar(30) NOT NULL DEFAULT 'NOT_STARTED',
    claim_id varchar(36),
    claimed_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    next_run_at timestamptz NOT NULL DEFAULT now(),
    publish_intent boolean NOT NULL DEFAULT false,
    published_at timestamptz,
    data_location varchar(2000),
    error_type varchar(100),
    acceptance_reason varchar(1000),
    acceptance_category varchar(100),
    acceptance_reminded_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE pull_delivery_key ADD FOREIGN KEY (current_transfer_id) REFERENCES pull_transfer(id);
ALTER TABLE transfer_notification ADD FOREIGN KEY (transfer_id) REFERENCES pull_transfer(id);
CREATE UNIQUE INDEX pull_notification_once ON transfer_notification(transfer_id) WHERE transfer_id IS NOT NULL;
CREATE INDEX pull_transfer_endpoint_claim ON pull_transfer(endpoint_id) WHERE claim_id IS NOT NULL;

CREATE TABLE pull_task_unit (
    task_id varchar(36) NOT NULL REFERENCES transfer_task(id),
    source_path varchar(1000) NOT NULL,
    transfer_id varchar(36) REFERENCES pull_transfer(id),
    status varchar(30) NOT NULL DEFAULT 'WAITING_SOURCE',
    error_type varchar(100),
    attempts integer NOT NULL DEFAULT 0,
    PRIMARY KEY (task_id, source_path)
);
CREATE TABLE pull_task_transfer (
    task_id varchar(36) NOT NULL REFERENCES transfer_task(id),
    transfer_id varchar(36) NOT NULL REFERENCES pull_transfer(id),
    PRIMARY KEY (task_id, transfer_id)
);
CREATE TABLE pull_file (
    transfer_id varchar(36) NOT NULL REFERENCES pull_transfer(id),
    relative_path varchar(1000) NOT NULL,
    size_bytes bigint NOT NULL,
    expected_hash varchar(64),
    actual_hash varchar(64),
    downloaded_bytes bigint NOT NULL DEFAULT 0,
    verified boolean NOT NULL DEFAULT false,
    PRIMARY KEY (transfer_id, relative_path)
);
CREATE TABLE pull_receipt (
    id bigserial PRIMARY KEY,
    transfer_id varchar(36) NOT NULL REFERENCES pull_transfer(id),
    recipient_id varchar(100) NOT NULL,
    payload text NOT NULL,
    outcome varchar(30) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
