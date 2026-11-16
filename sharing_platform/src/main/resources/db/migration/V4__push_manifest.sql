CREATE TABLE transfer_file (
    task_id varchar(36) NOT NULL REFERENCES transfer_task(id),
    product_id varchar(100) NOT NULL,
    relative_path varchar(1000) NOT NULL,
    size_bytes bigint NOT NULL,
    sha256 varchar(64) NOT NULL,
    source_modified_millis bigint NOT NULL,
    uploaded_bytes bigint NOT NULL DEFAULT 0,
    verified boolean NOT NULL DEFAULT false,
    PRIMARY KEY (task_id, product_id, relative_path)
);

CREATE TABLE transfer_directory (
    task_id varchar(36) NOT NULL REFERENCES transfer_task(id),
    product_id varchar(100) NOT NULL,
    relative_path varchar(1000) NOT NULL,
    PRIMARY KEY (task_id, product_id, relative_path)
);

ALTER TABLE transfer_task ADD COLUMN push_manifest_sha256 varchar(64);
ALTER TABLE transfer_task ADD COLUMN push_source_root varchar(1000);
ALTER TABLE transfer_task ADD COLUMN push_target_root varchar(1000);
ALTER TABLE transfer_task ADD COLUMN push_publish_intent boolean NOT NULL DEFAULT false;
