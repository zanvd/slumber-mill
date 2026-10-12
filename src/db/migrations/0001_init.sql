-- API keys are high-entropy random tokens, so plain SHA-256 is enough.
-- Webhook secrets can't be hashed, because signing needs the secret itself,
-- so they're encrypted.
CREATE TABLE IF NOT EXISTS tenants (
    id                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    slug               VARCHAR(64)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL,  -- immutable
    api_key_hash       BINARY(32)      NOT NULL,
    webhook_url        VARCHAR(2048)   NULL,
    webhook_secret_enc VARBINARY(128)  NULL,  -- key ID, nonce, AES-256-GCM ciphertext and tag
    target_lufs        DECIMAL(4, 1)   NOT NULL DEFAULT -16.0,
    created_at         DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uq_tenants_slug (slug),
    UNIQUE KEY uq_tenants_api_key_hash (api_key_hash)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- An asset is also its own unit of work: status is the lease state.
CREATE TABLE IF NOT EXISTS assets (
    id                   BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    public_id            BINARY(16)      NOT NULL,       -- UUIDv7
    tenant_id            BIGINT UNSIGNED NOT NULL,
    -- Compared byte for byte: under utf8mb4_0900_ai_ci 'AbC' = 'abc' and 'straße' = 'strasse'.
    idempotency_key      VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NULL,
    title                VARCHAR(255)    NOT NULL,
    status               ENUM ('queued','processing','ready','failed') NOT NULL DEFAULT 'queued',
    attempts             SMALLINT UNSIGNED NOT NULL DEFAULT 0,       -- also the fencing token
    run_after            DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    locked_by            VARCHAR(128)    NULL,
    lease_expires_at     DATETIME(6)     NULL,
    source_sha256        BINARY(32)      NOT NULL,
    source_bytes         BIGINT UNSIGNED NOT NULL,
    source_codec         VARCHAR(32)     NULL,
    duration_ms          BIGINT UNSIGNED NULL,
    target_lufs          DECIMAL(4, 1)   NOT NULL,       -- snapshot of the tenant's target at upload
    input_lufs           DECIMAL(5, 2)   NULL,
    input_true_peak_dbtp DECIMAL(5, 2)   NULL,
    input_lra_lu         DECIMAL(5, 2)   NULL,
    input_max_momentary  DECIMAL(5, 2)   NULL,           -- for the sudden-loud-event warning
    warnings             SET ('sudden_loud_event','dynamic_normalization','source_may_clip') NOT NULL DEFAULT '',
    error                VARCHAR(1024)   NULL,           -- tenant-safe message when failed
    last_error           VARCHAR(2048)   NULL,           -- internal detail, never in the API
    created_at           DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at           DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uq_assets_public_id (public_id),
    UNIQUE KEY uq_assets_tenant_idempotency (tenant_id, idempotency_key),  -- NULLs don't collide
    KEY ix_assets_tenant_id (tenant_id, id),                                -- keyset pagination
    KEY ix_assets_claim (status, run_after),
    KEY ix_assets_lease (status, lease_expires_at),                         -- reaper
    CONSTRAINT fk_assets_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- Loudness is measured per rendition: a mono downmix of the same master can
-- differ from the stereo version by about 3 LU.
CREATE TABLE IF NOT EXISTS renditions (
    id              BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    tenant_id       BIGINT UNSIGNED  NOT NULL,
    asset_id        BIGINT UNSIGNED  NOT NULL,
    profile         ENUM ('aac_128k','aac_64k') NOT NULL,
    codec           ENUM ('aac')     NOT NULL,
    bitrate_kbps    INT UNSIGNED     NOT NULL,
    channels        TINYINT UNSIGNED NOT NULL,
    bytes           BIGINT UNSIGNED  NOT NULL,
    sha256          BINARY(32)       NOT NULL,
    lufs            DECIMAL(5, 2)    NOT NULL,
    true_peak_dbtp  DECIMAL(5, 2)    NOT NULL,
    lra_lu          DECIMAL(5, 2)    NOT NULL,
    normalization   ENUM ('linear', 'dynamic') NOT NULL,  -- loudnorm can fall back silently
    created_at      DATETIME(6)      NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uq_renditions_asset_profile (asset_id, profile),  -- retries upsert, never duplicate
    KEY ix_renditions_tenant (tenant_id),
    CONSTRAINT fk_renditions_asset FOREIGN KEY (asset_id) REFERENCES assets (id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- The outbox: written in the same transaction as the asset change it records,
-- read and acknowledged by the webhook relay.
CREATE TABLE IF NOT EXISTS asset_events (
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    event_id    BINARY(16)      NOT NULL,       -- UUIDv7; becomes the webhook's event ID
    tenant_id   BIGINT UNSIGNED NOT NULL,
    asset_id    BIGINT UNSIGNED NOT NULL,
    type        ENUM ('asset.ready','asset.failed') NOT NULL,
    created_at  DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    relayed_at  DATETIME(6)     NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_asset_events_event_id (event_id),
    KEY ix_asset_events_pending (relayed_at, id),
    CONSTRAINT fk_asset_events_asset FOREIGN KEY (asset_id) REFERENCES assets (id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id               BIGINT UNSIGNED   NOT NULL AUTO_INCREMENT,
    event_id         BINARY(16)        NOT NULL,   -- one delivery per event, however often it's relayed
    tenant_id        BIGINT UNSIGNED   NOT NULL,
    body             MEDIUMBLOB        NOT NULL,   -- the exact bytes that get signed
    state            ENUM ('queued','sending','delivered','failed') NOT NULL DEFAULT 'queued',
    attempts         SMALLINT UNSIGNED NOT NULL DEFAULT 0,   -- also the fencing token
    run_after        DATETIME(6)       NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    locked_by        VARCHAR(128)      NULL,
    lease_expires_at DATETIME(6)       NULL,
    last_error       VARCHAR(2048)     NULL,
    created_at       DATETIME(6)       NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at       DATETIME(6)       NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uq_webhook_deliveries_event_id (event_id),
    KEY ix_webhook_deliveries_claim (state, run_after),
    KEY ix_webhook_deliveries_lease (state, lease_expires_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
