-- Existing tenants keep their stored target. INSTANT accepts only LOCK=DEFAULT.
ALTER TABLE tenants
    ALTER COLUMN target_lufs SET DEFAULT -20.0,
    ALGORITHM = INSTANT, LOCK = DEFAULT;
