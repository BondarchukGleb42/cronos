-- Run after projects.sql. Retain original project data, but prevent its implicit reuse.
ALTER TABLE projects ADD COLUMN IF NOT EXISTS context_excluded boolean NOT NULL DEFAULT false;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS context_reset_revision bigint NOT NULL DEFAULT 0;
