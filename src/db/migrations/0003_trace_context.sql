-- The trace context of an upload a Checkly check sent, carried with its work to the encode and webhook pools.
-- MySQL has no ADD COLUMN IF NOT EXISTS, so each column is added only where it's missing, which keeps the
-- file re-runnable. INSTANT accepts only LOCK=DEFAULT.
SET @add_trace_context = IF(EXISTS(SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'assets' AND column_name = 'trace_context'),
    'DO 0',
    'ALTER TABLE assets ADD COLUMN trace_context VARCHAR(512) CHARACTER SET ascii NULL, ALGORITHM = INSTANT, LOCK = DEFAULT');
PREPARE add_trace_context FROM @add_trace_context;
EXECUTE add_trace_context;
DEALLOCATE PREPARE add_trace_context;

SET @add_trace_context = IF(EXISTS(SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'asset_events' AND column_name = 'trace_context'),
    'DO 0',
    'ALTER TABLE asset_events ADD COLUMN trace_context VARCHAR(512) CHARACTER SET ascii NULL, ALGORITHM = INSTANT, LOCK = DEFAULT');
PREPARE add_trace_context FROM @add_trace_context;
EXECUTE add_trace_context;
DEALLOCATE PREPARE add_trace_context;

SET @add_trace_context = IF(EXISTS(SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'webhook_deliveries' AND column_name = 'trace_context'),
    'DO 0',
    'ALTER TABLE webhook_deliveries ADD COLUMN trace_context VARCHAR(512) CHARACTER SET ascii NULL, ALGORITHM = INSTANT, LOCK = DEFAULT');
PREPARE add_trace_context FROM @add_trace_context;
EXECUTE add_trace_context;
DEALLOCATE PREPARE add_trace_context;

SET @add_trace_context = NULL;
