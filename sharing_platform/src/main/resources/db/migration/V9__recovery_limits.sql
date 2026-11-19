ALTER TABLE transfer_task ADD COLUMN reconcile_attempts integer NOT NULL DEFAULT 0 CHECK (reconcile_attempts >= 0);
ALTER TABLE pull_transfer ADD COLUMN reconcile_attempts integer NOT NULL DEFAULT 0 CHECK (reconcile_attempts >= 0);
