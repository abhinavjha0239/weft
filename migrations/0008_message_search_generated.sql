-- Make message.search_tsv a STORED generated column (ADR-010 S-3, simplified):
-- every message — native and imported alike — is full-text indexed
-- automatically from its source, with zero writer maintenance. Adding a
-- generated column computes it for all existing rows, so imported history
-- becomes searchable immediately.
--
-- Migration 0004 created search_tsv as a plain tsvector + its GIN index;
-- dropping the column drops that index, so we recreate it.

ALTER TABLE message DROP COLUMN search_tsv;

ALTER TABLE message
    ADD COLUMN search_tsv tsvector
    GENERATED ALWAYS AS (to_tsvector('english', source)) STORED;

CREATE INDEX message_search_idx ON message USING gin (search_tsv);
