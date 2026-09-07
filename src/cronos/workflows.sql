-- Explicit personal workflows. User/project deletion cascades all workflow content.
CREATE TABLE IF NOT EXISTS workflows (
  id uuid PRIMARY KEY, user_id bigint NOT NULL, project_id uuid NOT NULL,
  kind text NOT NULL CHECK(kind IN ('nutrition','training','learning','content','wellbeing')),
  parameters jsonb NOT NULL CHECK(jsonb_typeof(parameters)='object'),
  plan jsonb NOT NULL CHECK(jsonb_typeof(plan)='object'),
  revision bigint NOT NULL DEFAULT 1 CHECK(revision>0),
  context_revision bigint NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(user_id,project_id), UNIQUE(user_id,project_id,id),
  FOREIGN KEY(user_id,project_id) REFERENCES projects(user_id,id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS workflow_observations (
  id uuid PRIMARY KEY, user_id bigint NOT NULL, project_id uuid NOT NULL, workflow_id uuid NOT NULL,
  revision bigint NOT NULL, observation jsonb NOT NULL CHECK(jsonb_typeof(observation)='object'),
  observed_at timestamptz NOT NULL DEFAULT now(), created_at timestamptz NOT NULL DEFAULT now(),
  origin_conversation_id uuid, run_id uuid,
  UNIQUE(user_id,workflow_id,revision),
  FOREIGN KEY(user_id,project_id,workflow_id) REFERENCES workflows(user_id,project_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,origin_conversation_id) REFERENCES conversations(user_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,run_id) REFERENCES runs(user_id,id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS workflow_observations_order ON workflow_observations(user_id,workflow_id,revision DESC);
CREATE TABLE IF NOT EXISTS workflow_plans (
  id uuid PRIMARY KEY, user_id bigint NOT NULL, project_id uuid NOT NULL, workflow_id uuid NOT NULL,
  revision bigint NOT NULL, parameters jsonb NOT NULL, plan jsonb NOT NULL,
  origin_conversation_id uuid, run_id uuid, created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(user_id,workflow_id,revision),
  FOREIGN KEY(user_id,project_id,workflow_id) REFERENCES workflows(user_id,project_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,origin_conversation_id) REFERENCES conversations(user_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,run_id) REFERENCES runs(user_id,id) ON DELETE CASCADE
);
-- Keep only operation/generation references across an explicit post-forget restart.
-- No FK to workflows: deleting an obsolete generation must not enable an old retry.
CREATE TABLE IF NOT EXISTS workflow_operations (
  user_id bigint NOT NULL, source_key text NOT NULL, project_id uuid NOT NULL, workflow_id uuid NOT NULL,
  kind text NOT NULL CHECK(kind IN ('start','observe','replan')),
  origin_conversation_id uuid, run_id uuid, created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(user_id,source_key),
  FOREIGN KEY(user_id,project_id) REFERENCES projects(user_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,origin_conversation_id) REFERENCES conversations(user_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,run_id) REFERENCES runs(user_id,id) ON DELETE CASCADE
);
DO $$ DECLARE t text; BEGIN
  FOREACH t IN ARRAY ARRAY['workflows','workflow_observations','workflow_plans','workflow_operations'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
    IF NOT EXISTS(SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename=t AND policyname='owner_access') THEN
      EXECUTE format('CREATE POLICY owner_access ON %I USING(user_id=nullif(current_setting(''app.user_id'',true),'''')::bigint) WITH CHECK(user_id=nullif(current_setting(''app.user_id'',true),'''')::bigint)',t);
    END IF;
    EXECUTE format('GRANT SELECT,INSERT,UPDATE,DELETE ON %I TO cronos_app',t);
  END LOOP;
END $$;
