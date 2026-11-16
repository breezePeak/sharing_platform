ALTER TABLE transfer_task ADD COLUMN push_manifest text;
ALTER TABLE transfer_task ADD COLUMN push_manifest_intent boolean NOT NULL DEFAULT false;
ALTER TABLE transfer_task ADD COLUMN push_route_snapshot text;
ALTER TABLE transfer_file ADD COLUMN upload_traffic_bytes bigint NOT NULL DEFAULT 0;
ALTER TABLE transfer_file ADD COLUMN readback_bytes bigint NOT NULL DEFAULT 0;
-- Old executions could only write a temporary manifest after verifying all business files.
UPDATE transfer_task t SET push_manifest_intent=true
WHERE push_manifest_sha256 IS NOT NULL AND (push_publish_intent OR
    (EXISTS (SELECT 1 FROM transfer_file f WHERE f.task_id=t.id) AND
     NOT EXISTS (SELECT 1 FROM transfer_file f WHERE f.task_id=t.id AND NOT f.verified)));
