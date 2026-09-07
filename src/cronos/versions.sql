-- Apply after schema.sql and projects.sql. Files remain in artifacts/PVC.
CREATE TABLE IF NOT EXISTS artifact_version_families (
  user_id bigint NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  id uuid NOT NULL,
  root_artifact_id uuid NOT NULL,
  next_version bigint NOT NULL DEFAULT 2 CHECK(next_version >= 2),
  PRIMARY KEY(user_id,id),
  UNIQUE(user_id,root_artifact_id),
  FOREIGN KEY(user_id,root_artifact_id) REFERENCES artifacts(user_id,id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS artifact_versions (
  user_id bigint NOT NULL,
  artifact_id uuid NOT NULL,
  family_id uuid NOT NULL,
  version bigint NOT NULL CHECK(version > 0),
  parent_artifact_id uuid,
  change_summary text NOT NULL DEFAULT '',
  project_id uuid,
  conversation_id uuid,
  run_id uuid,
  memory_revision bigint NOT NULL,
  context_excluded boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY(user_id,artifact_id),
  UNIQUE(user_id,family_id,version),
  UNIQUE(user_id,family_id,artifact_id),
  CHECK(parent_artifact_id IS NULL OR parent_artifact_id <> artifact_id),
  FOREIGN KEY(user_id,artifact_id) REFERENCES artifacts(user_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,family_id) REFERENCES artifact_version_families(user_id,id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,family_id,parent_artifact_id) REFERENCES artifact_versions(user_id,family_id,artifact_id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,project_id) REFERENCES projects(user_id,id),
  FOREIGN KEY(user_id,conversation_id) REFERENCES conversations(user_id,id),
  FOREIGN KEY(user_id,run_id) REFERENCES runs(user_id,id)
);

CREATE TABLE IF NOT EXISTS artifact_version_heads (
  user_id bigint NOT NULL,
  family_id uuid NOT NULL,
  artifact_id uuid NOT NULL,
  PRIMARY KEY(user_id,family_id),
  FOREIGN KEY(user_id,family_id,artifact_id) REFERENCES artifact_versions(user_id,family_id,artifact_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS artifact_version_references (
  user_id bigint NOT NULL,
  artifact_id uuid NOT NULL,
  reference_artifact_id uuid NOT NULL,
  ordinal integer NOT NULL CHECK(ordinal >= 0),
  PRIMARY KEY(user_id,artifact_id,reference_artifact_id),
  UNIQUE(user_id,artifact_id,ordinal),
  CHECK(artifact_id <> reference_artifact_id),
  FOREIGN KEY(user_id,artifact_id) REFERENCES artifact_versions(user_id,artifact_id) ON DELETE CASCADE,
  FOREIGN KEY(user_id,reference_artifact_id) REFERENCES artifacts(user_id,id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS artifact_version_operations (
  user_id bigint NOT NULL,
  source_key text NOT NULL CHECK(length(btrim(source_key)) > 0),
  kind text NOT NULL CHECK(kind IN ('register','restore')),
  family_id uuid NOT NULL,
  artifact_id uuid NOT NULL,
  PRIMARY KEY(user_id,source_key),
  FOREIGN KEY(user_id,family_id,artifact_id) REFERENCES artifact_versions(user_id,family_id,artifact_id) ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION protect_artifact_version() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(NEW.user_id,NEW.artifact_id,NEW.family_id,NEW.version,NEW.parent_artifact_id,
         NEW.change_summary,NEW.memory_revision,NEW.created_at)
     IS DISTINCT FROM
     ROW(OLD.user_id,OLD.artifact_id,OLD.family_id,OLD.version,OLD.parent_artifact_id,
         OLD.change_summary,OLD.memory_revision,OLD.created_at) THEN
    RAISE EXCEPTION 'Artifact version identity and content are immutable';
  END IF;
  IF (OLD.project_id IS NOT NULL AND NEW.project_id IS NULL)
     OR (OLD.conversation_id IS NOT NULL AND NEW.conversation_id IS NULL)
     OR (OLD.run_id IS NOT NULL AND NEW.run_id IS NULL) THEN
    NEW.context_excluded := true;
  END IF;
  IF OLD.context_excluded AND NOT NEW.context_excluded THEN
    RAISE EXCEPTION 'Excluded version context cannot be restored';
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS artifact_version_immutable ON artifact_versions;
CREATE TRIGGER artifact_version_immutable BEFORE UPDATE ON artifact_versions
  FOR EACH ROW EXECUTE FUNCTION protect_artifact_version();

-- PostgreSQL 14 has no SET NULL(column) for a composite FK. Preserve the owner
-- and original files while excluding metadata derived from a deleted source.
CREATE OR REPLACE FUNCTION detach_artifact_version_source() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_TABLE_NAME = 'projects' THEN
    UPDATE artifact_versions SET project_id=NULL,context_excluded=true
      WHERE user_id=OLD.user_id AND project_id=OLD.id;
  ELSIF TG_TABLE_NAME = 'conversations' THEN
    UPDATE artifact_versions SET conversation_id=NULL,context_excluded=true
      WHERE user_id=OLD.user_id AND conversation_id=OLD.id;
  ELSIF TG_TABLE_NAME = 'runs' THEN
    UPDATE artifact_versions SET run_id=NULL,context_excluded=true
      WHERE user_id=OLD.user_id AND run_id=OLD.id;
  END IF;
  RETURN OLD;
END $$;
DROP TRIGGER IF EXISTS detach_version_project ON projects;
CREATE TRIGGER detach_version_project BEFORE DELETE ON projects
  FOR EACH ROW EXECUTE FUNCTION detach_artifact_version_source();
DROP TRIGGER IF EXISTS detach_version_conversation ON conversations;
CREATE TRIGGER detach_version_conversation BEFORE DELETE ON conversations
  FOR EACH ROW EXECUTE FUNCTION detach_artifact_version_source();
DROP TRIGGER IF EXISTS detach_version_run ON runs;
CREATE TRIGGER detach_version_run BEFORE DELETE ON runs
  FOR EACH ROW EXECUTE FUNCTION detach_artifact_version_source();

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['artifact_version_families','artifact_versions','artifact_version_heads',
                          'artifact_version_references','artifact_version_operations'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
    IF NOT EXISTS(SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename=t AND policyname='owner_access') THEN
      EXECUTE format('CREATE POLICY owner_access ON %I USING (user_id=nullif(current_setting(''app.user_id'',true),'''')::bigint) WITH CHECK (user_id=nullif(current_setting(''app.user_id'',true),'''')::bigint)',t);
    END IF;
    EXECUTE format('GRANT SELECT,INSERT,UPDATE,DELETE ON %I TO cronos_app',t);
  END LOOP;
END $$;
