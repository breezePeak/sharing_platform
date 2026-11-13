CREATE TABLE platform_metadata (
    name VARCHAR(100) PRIMARY KEY,
    value VARCHAR(500) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO platform_metadata (name, value) VALUES ('platform', 'sharing-platform');
