-- Run after projects.sql and memory_privacy.sql. Existing memories become global.
ALTER TABLE memory ADD COLUMN IF NOT EXISTS scope text NOT NULL DEFAULT 'global';
ALTER TABLE memory ADD COLUMN IF NOT EXISTS conversation_id uuid;
ALTER TABLE memory ADD COLUMN IF NOT EXISTS project_id uuid;
ALTER TABLE memory ADD COLUMN IF NOT EXISTS expires_at timestamptz;
ALTER TABLE memory ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'active';
ALTER TABLE memory ADD COLUMN IF NOT EXISTS revision bigint NOT NULL DEFAULT 1;
ALTER TABLE memory ADD COLUMN IF NOT EXISTS supersedes_id uuid;
ALTER TABLE memory ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

-- Equal facts in different contexts are independent. Inactive versions may coexist.
ALTER TABLE memory DROP CONSTRAINT IF EXISTS memory_user_id_content_key;
CREATE UNIQUE INDEX IF NOT EXISTS memory_owner_id ON memory(user_id,id);
CREATE UNIQUE INDEX IF NOT EXISTS memory_active_global_content ON memory(user_id,content)
  WHERE scope='global' AND status='active';
CREATE UNIQUE INDEX IF NOT EXISTS memory_active_conversation_content ON memory(user_id,conversation_id,content)
  WHERE scope='conversation' AND status='active';
CREATE UNIQUE INDEX IF NOT EXISTS memory_active_project_content ON memory(user_id,project_id,content)
  WHERE scope='project' AND status='active';
CREATE INDEX IF NOT EXISTS memory_context ON memory(user_id,scope,status,updated_at DESC);

DO $$ BEGIN
  IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='memory'::regclass AND conname='memory_scope_valid') THEN
    ALTER TABLE memory ADD CONSTRAINT memory_scope_valid CHECK(
      (scope='global' AND conversation_id IS NULL AND project_id IS NULL) OR
      (scope='conversation' AND conversation_id IS NOT NULL AND project_id IS NULL) OR
      (scope='project' AND project_id IS NOT NULL AND conversation_id IS NULL));
    ALTER TABLE memory ADD CONSTRAINT memory_status_valid CHECK(status IN ('active','superseded','inactive'));
    ALTER TABLE memory ADD CONSTRAINT memory_revision_valid CHECK(revision>0);
    ALTER TABLE memory ADD CONSTRAINT memory_conversation_owner
      FOREIGN KEY(user_id,conversation_id) REFERENCES conversations(user_id,id) ON DELETE CASCADE;
    ALTER TABLE memory ADD CONSTRAINT memory_project_owner
      FOREIGN KEY(user_id,project_id) REFERENCES projects(user_id,id) ON DELETE CASCADE;
    ALTER TABLE memory ADD CONSTRAINT memory_predecessor
      FOREIGN KEY(supersedes_id) REFERENCES memory(id) ON DELETE SET NULL;
  END IF;
END $$;

-- The simple self-FK can safely clear a deleted predecessor without clearing user_id.
-- This trigger additionally enforces owner and scope, including writes through SQL.
CREATE OR REPLACE FUNCTION validate_memory_predecessor() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.supersedes_id IS NOT NULL AND NOT EXISTS(
    SELECT 1 FROM memory previous WHERE previous.id=NEW.supersedes_id
      AND previous.user_id=NEW.user_id AND previous.scope=NEW.scope
      AND previous.conversation_id IS NOT DISTINCT FROM NEW.conversation_id
      AND previous.project_id IS NOT DISTINCT FROM NEW.project_id
      AND previous.id<>NEW.id
  ) THEN
    RAISE EXCEPTION 'Memory predecessor is unavailable in this scope' USING ERRCODE='23503';
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS memory_predecessor_owner ON memory;
CREATE TRIGGER memory_predecessor_owner
  BEFORE INSERT OR UPDATE OF supersedes_id,user_id,scope,conversation_id,project_id ON memory
  FOR EACH ROW EXECUTE FUNCTION validate_memory_predecessor();

-- Idempotency receipts contain references, never another copy of a memory's content.
CREATE TABLE IF NOT EXISTS memory_operations (
  user_id bigint NOT NULL,
  source_key text NOT NULL,
  memory_id uuid NOT NULL,
  kind text NOT NULL CHECK(kind IN ('write','revise')),
  revision bigint NOT NULL,
  conversation_id uuid,
  run_id uuid,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(user_id,source_key),
  FOREIGN KEY(user_id,memory_id) REFERENCES memory(user_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,conversation_id) REFERENCES conversations(user_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,run_id) REFERENCES runs(user_id,id) ON DELETE CASCADE
);
ALTER TABLE memory_operations ENABLE ROW LEVEL SECURITY;
DO $$ BEGIN
  IF NOT EXISTS(SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename='memory_operations' AND policyname='owner_access') THEN
    CREATE POLICY owner_access ON memory_operations
      USING(user_id=nullif(current_setting('app.user_id',true),'')::bigint)
      WITH CHECK(user_id=nullif(current_setting('app.user_id',true),'')::bigint);
  END IF;
END $$;
GRANT SELECT,INSERT,UPDATE,DELETE ON memory_operations TO cronos_app;
