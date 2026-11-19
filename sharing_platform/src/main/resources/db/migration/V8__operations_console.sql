ALTER TABLE transfer_task ADD COLUMN read_only_recheck boolean NOT NULL DEFAULT false;
ALTER TABLE transfer_task ADD COLUMN current_file varchar(1200);
ALTER TABLE transfer_file ADD COLUMN effective_uploaded_bytes bigint NOT NULL DEFAULT 0;
UPDATE transfer_file SET effective_uploaded_bytes=uploaded_bytes;
ALTER TABLE pull_transfer ADD COLUMN current_file varchar(1000);

CREATE TABLE console_operation (
    id varchar(36) PRIMARY KEY,
    actor varchar(100) NOT NULL,
    object_id varchar(36) NOT NULL,
    action varchar(40) NOT NULL,
    before_state varchar(100),
    after_state varchar(100),
    result varchar(30) NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX console_operation_object ON console_operation(object_id,occurred_at DESC);
CREATE INDEX transfer_task_console_recent ON transfer_task(created_at DESC,id);
CREATE INDEX transfer_task_console_endpoint ON transfer_task(endpoint_id,created_at DESC,id);
