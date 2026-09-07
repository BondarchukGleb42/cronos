-- Run after schema.sql, in the same migration transaction.
-- Composite foreign keys enforce ownership even outside the Python helpers.
CREATE UNIQUE INDEX IF NOT EXISTS conversations_owner_id ON conversations(user_id,id);
CREATE UNIQUE INDEX IF NOT EXISTS artifacts_owner_id ON artifacts(user_id,id);
CREATE UNIQUE INDEX IF NOT EXISTS runs_owner_id ON runs(user_id,id);

CREATE TABLE IF NOT EXISTS projects (
  id uuid PRIMARY KEY,
  user_id bigint NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  name text NOT NULL CHECK(length(btrim(name)) > 0),
  goal text NOT NULL DEFAULT '',
  status text NOT NULL DEFAULT 'active' CHECK(status IN ('active','paused','completed')),
  state jsonb NOT NULL DEFAULT '{"summary":"","constraints":[],"decisions":[],"open_questions":[],"next_step":""}',
  revision bigint NOT NULL DEFAULT 1 CHECK(revision > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(user_id,id),
  CHECK(jsonb_typeof(state)='object')
);
CREATE INDEX IF NOT EXISTS projects_owner_status ON projects(user_id,status,updated_at DESC);

CREATE TABLE IF NOT EXISTS project_conversations (
  user_id bigint NOT NULL,
  project_id uuid NOT NULL,
  conversation_id uuid NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(user_id,conversation_id),
  FOREIGN KEY(user_id,project_id) REFERENCES projects(user_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,conversation_id) REFERENCES conversations(user_id,id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS project_conversations_project ON project_conversations(user_id,project_id);

CREATE TABLE IF NOT EXISTS project_artifacts (
  user_id bigint NOT NULL,
  project_id uuid NOT NULL,
  artifact_id uuid NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(user_id,project_id,artifact_id),
  FOREIGN KEY(user_id,project_id) REFERENCES projects(user_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,artifact_id) REFERENCES artifacts(user_id,id) ON DELETE CASCADE
);

-- Store only the changed fields, never copied snapshots from other conversations.
-- A chat wipe removes that chat's provenance/history while the shared project survives.
CREATE TABLE IF NOT EXISTS project_changes (
  id uuid PRIMARY KEY,
  user_id bigint NOT NULL,
  project_id uuid,
  revision bigint NOT NULL,
  kind text NOT NULL CHECK(kind IN ('create','update','attach_conversation','detach_conversation','attach_artifact')),
  source_key text NOT NULL,
  conversation_id uuid,
  run_id uuid,
  patch jsonb NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(user_id,source_key),
  CHECK(kind='detach_conversation' OR project_id IS NOT NULL),
  FOREIGN KEY(user_id,project_id) REFERENCES projects(user_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,conversation_id) REFERENCES conversations(user_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,run_id) REFERENCES runs(user_id,id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS project_changes_project ON project_changes(user_id,project_id,revision DESC);

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['projects','project_conversations','project_artifacts','project_changes'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
    IF NOT EXISTS(SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename=t AND policyname='owner_access') THEN
      EXECUTE format('CREATE POLICY owner_access ON %I USING (user_id = nullif(current_setting(''app.user_id'',true),'''')::bigint) WITH CHECK (user_id = nullif(current_setting(''app.user_id'',true),'''')::bigint)',t);
    END IF;
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON %I TO cronos_app',t);
  END LOOP;
END $$;
