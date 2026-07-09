-- Work tracking: statuses, types, fields, work items, links, sprints, views.
-- Design: ADR-009 (resolution derived, single field-visibility system,
-- LexoRank, typed hierarchy levels), ADR-008 C-6 (boards=Views, components),
-- ADR-001 D2 (WorkItem owns a Thread — the fusion), F-21 (rank contexts).

CREATE TABLE status_set (
    id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id BIGINT NOT NULL REFERENCES org (id),
    name   TEXT NOT NULL,
    origin_system TEXT, origin_id TEXT, origin_meta JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE space
    ADD CONSTRAINT space_status_set_fk
    FOREIGN KEY (status_set_id) REFERENCES status_set (id);

CREATE TABLE status (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    status_set_id BIGINT NOT NULL REFERENCES status_set (id),
    name          TEXT NOT NULL,
    -- 1 todo · 2 in_progress · 3 done. Resolution DERIVES from this (W-3):
    -- an item is resolved iff its status category = done. No resolution trap.
    category      SMALLINT NOT NULL,
    position      INT NOT NULL DEFAULT 0,
    locks_editing BOOLEAN NOT NULL DEFAULT false, -- W-4 workflow property
    UNIQUE (status_set_id, name)
);

CREATE TABLE item_type (
    id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    space_id BIGINT NOT NULL REFERENCES space (id),
    name     TEXT NOT NULL,
    icon     TEXT,
    -- W-1: hierarchy as an integer level on the type, not a hardcoded 3-tier:
    -- -1 sub-task · 0 standard · 1 epic · 2+ above-epic (config, not schema).
    level    SMALLINT NOT NULL DEFAULT 0,
    origin_system TEXT, origin_id TEXT, origin_meta JSONB,
    UNIQUE (space_id, name)
);

-- W-2: ONE field system (applies_to + required + visibility), replacing
-- Jira's 4-way context×screen×config×workflow intersection. `required` is
-- enforced at the DOMAIN layer — a real constraint, not UI-only.
CREATE TABLE field_def (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    space_id   BIGINT NOT NULL REFERENCES space (id),
    key        TEXT NOT NULL CHECK (key ~ '^[a-z][a-z0-9_]{0,62}$'),
    name       TEXT NOT NULL,
    -- Typed taxonomy (ADR-009 W-2 + F-18): text_short, text_long, number,
    -- date, datetime, checkbox, radio, select, multi_select, cascading,
    -- url, user, multi_user, group, version, labels, project, readonly,
    -- import_id. Registry constant lives in Go.
    field_type SMALLINT NOT NULL,
    applies_to BIGINT[] NOT NULL DEFAULT '{}', -- item_type ids; empty = all
    required   BOOLEAN NOT NULL DEFAULT false,
    -- 1 always · 2 edit-only · 3 hidden
    visibility SMALLINT NOT NULL DEFAULT 1,
    options    JSONB NOT NULL DEFAULT '{}',    -- select options, defaults
    position   INT NOT NULL DEFAULT 0,
    origin_system TEXT, origin_id TEXT, origin_meta JSONB,
    UNIQUE (space_id, key)
);

-- F-21: rank sequences are scoped to a context — normally one per Space;
-- cross-project boards get a shared context so their manual order survives.
CREATE TABLE rank_context (
    id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id BIGINT NOT NULL REFERENCES org (id),
    name   TEXT
);

CREATE TABLE work_item (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id      BIGINT NOT NULL REFERENCES org (id),
    space_id    BIGINT NOT NULL REFERENCES space (id),
    -- Display key = space.key || '-' || key_no; relationships NEVER use it
    -- (ADR-008: stable internal ids survive moves — fixes Jira's move bug).
    key_no      INT NOT NULL,
    type_id     BIGINT NOT NULL REFERENCES item_type (id),
    status_id   BIGINT NOT NULL REFERENCES status (id),
    title       TEXT NOT NULL,
    -- ADR-001 D2, the fusion: every WorkItem owns exactly one Thread.
    thread_id   BIGINT NOT NULL UNIQUE REFERENCES thread (id),
    assignee_id BIGINT REFERENCES user_account (id),
    reporter_id BIGINT REFERENCES user_account (id),
    priority    SMALLINT,
    parent_id   BIGINT REFERENCES work_item (id), -- typed levels validate depth
    -- LexoRank-style sparse ordering; byte-order collation is the point.
    rank            TEXT COLLATE "C",
    rank_context_id BIGINT NOT NULL REFERENCES rank_context (id),
    sprint_id   BIGINT, -- FK added below
    story_points NUMERIC(6,2),
    original_estimate_secs INT,
    remaining_estimate_secs INT,
    time_spent_secs INT NOT NULL DEFAULT 0, -- derived from work_log
    due_date    DATE,
    start_date  DATE,
    labels      TEXT[] NOT NULL DEFAULT '{}',
    flagged     BOOLEAN NOT NULL DEFAULT false,
    security_scope_id BIGINT, -- FK added below (P-4 VisibilityScope)
    fields      JSONB NOT NULL DEFAULT '{}', -- custom values, validated vs field_def
    -- W-3: derived from status category at transition time, never free-set.
    resolved_at TIMESTAMPTZ,
    -- 1 fixed · 2 wont_do · 3 duplicate · 4 unspecified (prompted on done)
    resolution_reason SMALLINT,
    votes_count INT NOT NULL DEFAULT 0,
    origin_system TEXT, origin_id TEXT, origin_meta JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    trashed_at  TIMESTAMPTZ,
    UNIQUE (space_id, key_no)
);
CREATE INDEX work_item_board_idx ON work_item (space_id, status_id) WHERE trashed_at IS NULL;
CREATE INDEX work_item_rank_idx ON work_item (rank_context_id, rank) WHERE trashed_at IS NULL;
CREATE INDEX work_item_assignee_open_idx
    ON work_item (assignee_id) WHERE resolved_at IS NULL AND trashed_at IS NULL;
CREATE INDEX work_item_parent_idx ON work_item (parent_id);
CREATE INDEX work_item_labels_idx ON work_item USING gin (labels);
CREATE INDEX work_item_fields_idx ON work_item USING gin (fields jsonb_path_ops);
CREATE UNIQUE INDEX work_item_origin_key
    ON work_item (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL;

CREATE TABLE link_type (
    id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id    BIGINT NOT NULL REFERENCES org (id),
    inward    TEXT NOT NULL,  -- "is blocked by"
    outward   TEXT NOT NULL,  -- "blocks"
    is_system BOOLEAN NOT NULL DEFAULT false,
    UNIQUE (org_id, outward)
);

-- Keyed by stable internal ids: links SURVIVE moves (ADR-008 C-6).
CREATE TABLE work_item_link (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id       BIGINT NOT NULL REFERENCES org (id),
    from_item_id BIGINT NOT NULL REFERENCES work_item (id),
    to_item_id   BIGINT NOT NULL REFERENCES work_item (id),
    link_type_id BIGINT NOT NULL REFERENCES link_type (id),
    created_by   BIGINT REFERENCES user_account (id),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (from_item_id, to_item_id, link_type_id)
);
CREATE INDEX work_item_link_to_idx ON work_item_link (to_item_id);

CREATE TABLE remote_link (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    work_item_id BIGINT NOT NULL REFERENCES work_item (id),
    url          TEXT NOT NULL,
    title        TEXT NOT NULL DEFAULT ''
);
CREATE INDEX remote_link_item_idx ON remote_link (work_item_id);

CREATE TABLE sprint (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id       BIGINT NOT NULL REFERENCES org (id),
    space_id     BIGINT NOT NULL REFERENCES space (id),
    name         TEXT NOT NULL,
    goal         TEXT NOT NULL DEFAULT '',
    -- 1 future · 2 active · 3 closed (verbs: ADR-006 3-way split)
    state        SMALLINT NOT NULL DEFAULT 1,
    starts_at    TIMESTAMPTZ,
    ends_at      TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    origin_system TEXT, origin_id TEXT, origin_meta JSONB
);
CREATE INDEX sprint_space_idx ON sprint (space_id, state);

ALTER TABLE work_item
    ADD CONSTRAINT work_item_sprint_fk FOREIGN KEY (sprint_id) REFERENCES sprint (id);
CREATE INDEX work_item_sprint_idx ON work_item (sprint_id) WHERE sprint_id IS NOT NULL;

-- Versions/releases (Milestone entity, ADR-001/008): items carry fix-versions
-- via the join table (multi-valued in Jira).
CREATE TABLE milestone (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id      BIGINT NOT NULL REFERENCES org (id),
    space_id    BIGINT NOT NULL REFERENCES space (id),
    name        TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    due_date    DATE,
    released_at TIMESTAMPTZ,
    origin_system TEXT, origin_id TEXT, origin_meta JSONB,
    UNIQUE (space_id, name)
);

CREATE TABLE work_item_milestone (
    work_item_id BIGINT NOT NULL REFERENCES work_item (id),
    milestone_id BIGINT NOT NULL REFERENCES milestone (id),
    PRIMARY KEY (work_item_id, milestone_id)
);

CREATE TABLE component (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    space_id      BIGINT NOT NULL REFERENCES space (id),
    name          TEXT NOT NULL,
    lead_user_id  BIGINT REFERENCES user_account (id),
    -- ADR-008 C-6: a component can set the default assignee.
    -- 1 none · 2 component-lead · 3 space-lead · 4 unassigned
    auto_assignee SMALLINT NOT NULL DEFAULT 1,
    origin_system TEXT, origin_id TEXT, origin_meta JSONB,
    UNIQUE (space_id, name)
);

CREATE TABLE work_item_component (
    work_item_id BIGINT NOT NULL REFERENCES work_item (id),
    component_id BIGINT NOT NULL REFERENCES component (id),
    PRIMARY KEY (work_item_id, component_id)
);

CREATE TABLE work_log (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id        BIGINT NOT NULL REFERENCES org (id),
    work_item_id  BIGINT NOT NULL REFERENCES work_item (id),
    author_id     BIGINT NOT NULL REFERENCES user_account (id),
    started_at    TIMESTAMPTZ NOT NULL,
    duration_secs INT NOT NULL,
    note          TEXT NOT NULL DEFAULT '',
    origin_system TEXT, origin_id TEXT, origin_meta JSONB
);
CREATE INDEX work_log_item_idx ON work_log (work_item_id);

-- P-4: named item-security levels per Space (Jira issue security).
CREATE TABLE visibility_scope (
    id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    space_id BIGINT NOT NULL REFERENCES space (id),
    name     TEXT NOT NULL,
    rule     JSONB NOT NULL, -- e.g. {"roles":["reporter","assignee"],"groups":[...]}
    origin_system TEXT, origin_id TEXT, origin_meta JSONB,
    UNIQUE (space_id, name)
);

ALTER TABLE work_item
    ADD CONSTRAINT work_item_security_fk
    FOREIGN KEY (security_scope_id) REFERENCES visibility_scope (id);

-- ADR-008 C-1 / ADR-010 S-4: a board IS a saved query + layout. Views are NOT
-- owned by a Space (they may span Spaces); a saved search IS a View.
CREATE TABLE view_def (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id       BIGINT NOT NULL REFERENCES org (id),
    workspace_id BIGINT REFERENCES workspace (id),
    owner_id     BIGINT NOT NULL REFERENCES user_account (id),
    name         TEXT NOT NULL,
    -- 1 list · 2 kanban · 3 timeline · 4 saved-search
    layout       SMALLINT NOT NULL DEFAULT 1,
    query        JSONB NOT NULL,              -- structured filter AST (S-4)
    config       JSONB NOT NULL DEFAULT '{}', -- columns/swimlanes/card rules
    subscription JSONB,                       -- digest on new matches (S-4)
    origin_system TEXT, origin_id TEXT, origin_meta JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX view_def_owner_idx ON view_def (owner_id);
