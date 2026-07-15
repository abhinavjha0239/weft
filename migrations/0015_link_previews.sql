-- P-15: link previews (unfurl). The cache is GLOBAL — no org_id — because a
-- URL's preview is objective content (title/description the page itself
-- publishes); orgs sharing one row is dedup, not a leak (the Zulip
-- per-server precedent). Keyed by sha256 hex of the exact URL string.
--
-- status: 1 ok · 2 failed (fetch/parse error, retried after a short TTL) ·
-- 3 disallowed (the egress guard refused the destination — cached as long
-- as ok rows, because guard verdicts don't flap).
CREATE TABLE link_preview (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url_hash    TEXT NOT NULL UNIQUE,
    url         TEXT NOT NULL,
    title       TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    -- The og:image URL as a STRING — never fetched or proxied server-side;
    -- rendering it raw leaks reader IPs, so the client era adds a camo
    -- proxy (recorded gap).
    image_url   TEXT NOT NULL DEFAULT '',
    site_name   TEXT NOT NULL DEFAULT '',
    status      SMALLINT NOT NULL,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ NOT NULL
);

-- Message association, written by the unfurl consumer. position preserves
-- document order (first N links per message). No FK to message: the message
-- lane owns message lifecycle; dead associations are reaped with it later.
CREATE TABLE message_link_preview (
    message_id BIGINT NOT NULL,
    preview_id BIGINT NOT NULL REFERENCES link_preview (id),
    position   SMALLINT NOT NULL,
    PRIMARY KEY (message_id, preview_id)
);
