-- P-17: message retention vacuum. retention_vacuumed_at marks a message
-- tombstoned by the retention lane (as opposed to a user delete via
-- message.deleted_at, which the retention lane also sets). It stays NULL for
-- every other message and lets the after-window purge lane find the rows it
-- may permanently remove. A vacuumed message keeps its content in place —
-- hidden from the product by deleted_at, still present for compliance export
-- and recovery — until the restore window elapses and the purge lane deletes
-- the row outright.
ALTER TABLE message ADD COLUMN retention_vacuumed_at TIMESTAMPTZ;
