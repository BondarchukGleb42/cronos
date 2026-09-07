-- User-authored declarations, never executable code. Definitions are immutable.
CREATE TABLE IF NOT EXISTS recipes (
  user_id bigint NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  id uuid NOT NULL,
  revision bigint NOT NULL CHECK(revision > 0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY(user_id,id)
);
CREATE TABLE IF NOT EXISTS recipe_versions (
  user_id bigint NOT NULL,
  recipe_id uuid NOT NULL,
  version bigint NOT NULL CHECK(version > 0),
  definition jsonb NOT NULL CHECK(jsonb_typeof(definition)='object'),
  memory_revision bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY(user_id,recipe_id,version),
  FOREIGN KEY(user_id,recipe_id) REFERENCES recipes(user_id,id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS recipe_applications (
  user_id bigint NOT NULL,
  id uuid NOT NULL,
  recipe_id uuid NOT NULL,
  version bigint NOT NULL,
  run_id uuid NOT NULL,
  conversation_id uuid NOT NULL,
  status text NOT NULL CHECK(status IN ('awaiting_input','ready','completed','superseded')),
  inputs jsonb NOT NULL CHECK(jsonb_typeof(inputs)='object'),
  missing_inputs jsonb NOT NULL CHECK(jsonb_typeof(missing_inputs)='array'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  completed_at timestamptz,
  PRIMARY KEY(user_id,id),
  FOREIGN KEY(user_id,recipe_id,version) REFERENCES recipe_versions(user_id,recipe_id,version) ON DELETE CASCADE,
  FOREIGN KEY(user_id,conversation_id) REFERENCES conversations(user_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,run_id) REFERENCES runs(user_id,id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS recipe_ready_run ON recipe_applications(user_id,run_id) WHERE status='ready';
CREATE UNIQUE INDEX IF NOT EXISTS recipe_waiting_conversation ON recipe_applications(user_id,conversation_id,recipe_id) WHERE status='awaiting_input';
CREATE TABLE IF NOT EXISTS recipe_operations (
  user_id bigint NOT NULL,
  source_key text NOT NULL CHECK(length(btrim(source_key)) > 0),
  kind text NOT NULL CHECK(kind IN ('save','apply','complete')),
  recipe_id uuid NOT NULL,
  version bigint NOT NULL,
  application_id uuid,
  PRIMARY KEY(user_id,source_key),
  FOREIGN KEY(user_id,recipe_id,version) REFERENCES recipe_versions(user_id,recipe_id,version) ON DELETE CASCADE,
  FOREIGN KEY(user_id,application_id) REFERENCES recipe_applications(user_id,id) ON DELETE CASCADE
);
-- Operation rows use a global primary key in the existing schema. Their owner,
-- run and successful result are checked inside the completion transaction.
CREATE TABLE IF NOT EXISTS recipe_application_receipts (
  user_id bigint NOT NULL,
  application_id uuid NOT NULL,
  operation_id text NOT NULL,
  tool text NOT NULL,
  artifact_id uuid,
  PRIMARY KEY(user_id,operation_id),
  FOREIGN KEY(user_id,application_id) REFERENCES recipe_applications(user_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,artifact_id) REFERENCES artifacts(user_id,id) ON DELETE CASCADE
);
CREATE OR REPLACE FUNCTION immutable_recipe_definition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW IS DISTINCT FROM OLD THEN
    RAISE EXCEPTION 'Recipe versions are immutable';
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS immutable_recipe_version ON recipe_versions;
CREATE TRIGGER immutable_recipe_version BEFORE UPDATE ON recipe_versions
  FOR EACH ROW EXECUTE FUNCTION immutable_recipe_definition();
DO $$ DECLARE t text; BEGIN
  FOREACH t IN ARRAY ARRAY['recipes','recipe_versions','recipe_applications','recipe_operations','recipe_application_receipts'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
    IF NOT EXISTS(SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename=t AND policyname='owner_access') THEN
      EXECUTE format('CREATE POLICY owner_access ON %I USING(user_id=nullif(current_setting(''app.user_id'',true),'''')::bigint) WITH CHECK(user_id=nullif(current_setting(''app.user_id'',true),'''')::bigint)',t);
    END IF;
    EXECUTE format('GRANT SELECT,INSERT,UPDATE,DELETE ON %I TO cronos_app',t);
  END LOOP;
END $$;
