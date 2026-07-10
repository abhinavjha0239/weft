-- user_group has carried provenance columns since 0002 but lacked the
-- partial unique origin index every other imported entity has; the
-- importer's idempotent upsert (ON CONFLICT on the origin index) needs it.
CREATE UNIQUE INDEX user_group_origin_key
    ON user_group (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL;
