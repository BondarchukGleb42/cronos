-- Project initiative shares the existing durable scheduler and delivery outbox.
CREATE UNIQUE INDEX IF NOT EXISTS schedules_owner_id ON schedules(user_id,id);
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS initiative_id uuid;

CREATE TABLE IF NOT EXISTS initiative_policies (
  id uuid PRIMARY KEY,
  user_id bigint NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  project_id uuid NOT NULL,
  schedule_id uuid NOT NULL UNIQUE,
  conversation_id uuid NOT NULL,
  purpose text NOT NULL,
  instruction text NOT NULL,
  allowed_tools text[] NOT NULL,
  status text NOT NULL DEFAULT 'active' CHECK(status IN ('active','paused')),
  revision bigint NOT NULL DEFAULT 1 CHECK(revision>0),
  context_reset_revision bigint NOT NULL,
  last_fingerprint text,
  source_key text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(user_id,id),
  UNIQUE(user_id,source_key),
  CHECK(allowed_tools <@ ARRAY['project_get','library_search','library_read','file_read',
    'table_analyze','web_search','file_create','skill_info']::text[]),
  FOREIGN KEY(user_id,project_id) REFERENCES projects(user_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,schedule_id) REFERENCES schedules(user_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,conversation_id) REFERENCES conversations(user_id,id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS initiative_policies_project ON initiative_policies(user_id,project_id);

CREATE TABLE IF NOT EXISTS initiative_operations (
  id uuid PRIMARY KEY,
  user_id bigint NOT NULL,
  initiative_id uuid NOT NULL,
  kind text NOT NULL CHECK(kind IN ('configure','feedback','decide')),
  source_key text NOT NULL,
  run_id uuid NOT NULL,
  conversation_id uuid NOT NULL,
  result jsonb NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(user_id,source_key),
  FOREIGN KEY(user_id,initiative_id) REFERENCES initiative_policies(user_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,run_id) REFERENCES runs(user_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,conversation_id) REFERENCES conversations(user_id,id) ON DELETE CASCADE
);

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['initiative_policies','initiative_operations'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
    IF NOT EXISTS(SELECT 1 FROM pg_policies WHERE tablename=t AND policyname='owner_access') THEN
      EXECUTE format('CREATE POLICY owner_access ON %I USING (user_id = nullif(current_setting(''app.user_id'',true),'''')::bigint) WITH CHECK (user_id = nullif(current_setting(''app.user_id'',true),'''')::bigint)',t);
    END IF;
  END LOOP;
END $$;
GRANT SELECT,INSERT,UPDATE,DELETE ON initiative_policies,initiative_operations TO cronos_app;
