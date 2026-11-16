CREATE TABLE transfer_task (
    id varchar(36) PRIMARY KEY,
    caller_id varchar(100) NOT NULL,
    external_task_id varchar(100) NOT NULL,
    entry_type varchar(10) NOT NULL,
    direction varchar(4) NOT NULL CHECK (direction IN ('PUSH', 'PULL')),
    endpoint_id varchar(36) NOT NULL REFERENCES endpoint(id),
    endpoint_revision_id varchar(36) NOT NULL REFERENCES endpoint_revision(id),
    notification_url varchar(1000) NOT NULL,
    canonical_request text NOT NULL,
    raw_request text NOT NULL,
    status varchar(30) NOT NULL,
    phase varchar(30) NOT NULL,
    acceptance_status varchar(30) NOT NULL DEFAULT 'NOT_STARTED',
    error_type varchar(30),
    error_detail varchar(1000),
    next_run_at timestamptz NOT NULL,
    claim_id varchar(36),
    claimed_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    version bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (caller_id, external_task_id)
);
CREATE INDEX transfer_task_due ON transfer_task(next_run_at) WHERE status IN ('QUEUED', 'RUNNING');

CREATE TABLE transfer_product (
    task_id varchar(36) NOT NULL REFERENCES transfer_task(id),
    position integer NOT NULL,
    product_id varchar(100) NOT NULL,
    product_path varchar(1000) NOT NULL,
    product_name varchar(200),
    PRIMARY KEY (task_id, position)
);

CREATE TABLE transfer_notification (
    id varchar(36) PRIMARY KEY,
    task_id varchar(36) NOT NULL REFERENCES transfer_task(id),
    stage varchar(30) NOT NULL,
    result_status varchar(30) NOT NULL,
    protocol varchar(10) NOT NULL,
    recipient_url varchar(1000) NOT NULL,
    payload text NOT NULL,
    payload_sha256 varchar(64) NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL,
    status varchar(30) NOT NULL,
    claim_id varchar(36),
    claimed_at timestamptz,
    ack text,
    last_error varchar(1000),
    created_at timestamptz NOT NULL,
    UNIQUE (task_id, stage)
);
CREATE INDEX transfer_notification_due ON transfer_notification(next_attempt_at) WHERE status IN ('PENDING', 'RETRY_WAIT', 'SENDING');
