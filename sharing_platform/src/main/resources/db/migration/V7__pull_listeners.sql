CREATE TABLE pull_listener (
    id varchar(36) PRIMARY KEY,
    name varchar(100) NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    period_seconds integer NOT NULL CHECK (period_seconds BETWEEN 1 AND 86400),
    initial_mode varchar(20) NOT NULL CHECK (initial_mode IN ('IMPORT_EXISTING','BASELINE_ONLY')),
    initialized_at timestamptz,
    initialized_revision integer,
    revision integer NOT NULL,
    edit_version integer NOT NULL DEFAULT 1,
    next_run_at timestamptz NOT NULL,
    manual_pending boolean NOT NULL DEFAULT false,
    last_started_at timestamptz
);

CREATE TABLE pull_listener_revision (
    listener_id varchar(36) NOT NULL REFERENCES pull_listener(id),
    revision integer NOT NULL,
    endpoint_id varchar(36) NOT NULL REFERENCES endpoint(id),
    source_id varchar(100) NOT NULL,
    source_path varchar(1000) NOT NULL,
    config text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (listener_id, revision)
);

CREATE TABLE pull_listener_run (
    id varchar(36) PRIMARY KEY,
    listener_id varchar(36) NOT NULL,
    revision integer NOT NULL,
    endpoint_revision_id varchar(36) NOT NULL REFERENCES endpoint_revision(id),
    trigger_type varchar(10) NOT NULL CHECK (trigger_type IN ('MANUAL','SCHEDULE')),
    status varchar(20) NOT NULL CHECK (status IN ('SCANNING','SUCCEEDED','FAILED','INTERRUPTED','BASELINED')),
    started_at timestamptz NOT NULL,
    heartbeat_at timestamptz NOT NULL,
    ended_at timestamptz,
    coalesced_reason varchar(40),
    error_type varchar(100),
    candidate_count integer NOT NULL DEFAULT 0,
    waiting_count integer NOT NULL DEFAULT 0,
    new_version_count integer NOT NULL DEFAULT 0,
    created_count integer NOT NULL DEFAULT 0,
    reused_count integer NOT NULL DEFAULT 0,
    created_task_count integer NOT NULL DEFAULT 0,
    reused_task_count integer NOT NULL DEFAULT 0,
    FOREIGN KEY (listener_id, revision) REFERENCES pull_listener_revision(listener_id, revision)
);
CREATE UNIQUE INDEX pull_listener_one_running ON pull_listener_run(listener_id) WHERE status='SCANNING';
CREATE INDEX pull_listener_due ON pull_listener(next_run_at) WHERE enabled OR manual_pending;
CREATE INDEX pull_listener_run_history ON pull_listener_run(listener_id, started_at DESC);

CREATE TABLE pull_listener_baseline (
    listener_id varchar(36) NOT NULL,
    revision integer NOT NULL,
    delivery_key varchar(64) NOT NULL,
    PRIMARY KEY (listener_id, delivery_key),
    FOREIGN KEY (listener_id, revision) REFERENCES pull_listener_revision(listener_id, revision)
);

ALTER TABLE pull_candidate ADD COLUMN first_observed_at timestamptz;
ALTER TABLE pull_candidate ADD COLUMN last_seen_at timestamptz;
UPDATE pull_candidate SET first_observed_at=observed_at,last_seen_at=observed_at;
CREATE TABLE pull_listener_candidate (
    listener_id varchar(36) NOT NULL,
    revision integer NOT NULL,
    candidate_key varchar(64) NOT NULL REFERENCES pull_candidate(candidate_key),
    last_run_id varchar(36) REFERENCES pull_listener_run(id),
    PRIMARY KEY (listener_id, revision, candidate_key),
    FOREIGN KEY (listener_id, revision) REFERENCES pull_listener_revision(listener_id, revision)
);

ALTER TABLE transfer_task ADD COLUMN trigger_type varchar(10) NOT NULL DEFAULT 'API' CHECK (trigger_type IN ('API','MANUAL','SCHEDULE'));
ALTER TABLE transfer_task ADD COLUMN listener_id varchar(36);
ALTER TABLE transfer_task ADD COLUMN listener_revision integer;
ALTER TABLE transfer_task ADD FOREIGN KEY (listener_id, listener_revision)
    REFERENCES pull_listener_revision(listener_id, revision);
ALTER TABLE transfer_task ADD CHECK ((listener_id IS NULL) = (listener_revision IS NULL));
CREATE TABLE pull_listener_run_task (
    run_id varchar(36) NOT NULL REFERENCES pull_listener_run(id),
    task_id varchar(36) NOT NULL REFERENCES transfer_task(id),
    PRIMARY KEY (run_id, task_id)
);
CREATE INDEX pull_transfer_version_lookup ON pull_transfer(endpoint_id, source_version, version_basis);
