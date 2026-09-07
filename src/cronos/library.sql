-- Search indexes contain lexemes from existing source rows, not a second content store.
-- RLS may evaluate non-leakproof full-text operators after the owner filter.
-- Keep that owner/current-conversation scan bounded without weakening RLS.
CREATE INDEX IF NOT EXISTS messages_visible_owner ON messages(user_id,conversation_id,id DESC)
  WHERE NOT excluded;

CREATE OR REPLACE FUNCTION cronos_library_fold(value text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT lower(translate(coalesce(value,''),
    'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ',
    'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'))
$$;

CREATE OR REPLACE FUNCTION cronos_library_message_body(value jsonb) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE jsonb_typeof(value)
    WHEN 'string' THEN value #>> '{}'
    WHEN 'array' THEN coalesce((
      SELECT string_agg(item->>'text', E'\n' ORDER BY ordinal)
      FROM jsonb_array_elements(value) WITH ORDINALITY AS items(item,ordinal)
      WHERE item->>'type'='text' AND jsonb_typeof(item->'text')='string'
    ),'')
    ELSE '' END
$$;

CREATE OR REPLACE FUNCTION cronos_library_project_body(name text, goal text, state jsonb)
RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT concat_ws(E'\n', name, goal, state->>'summary',
    CASE WHEN jsonb_typeof(state->'constraints')='array' THEN
      (SELECT string_agg(item #>> '{}', E'\n') FROM jsonb_array_elements(state->'constraints') item) END,
    CASE WHEN jsonb_typeof(state->'decisions')='array' THEN
      (SELECT string_agg(item #>> '{}', E'\n') FROM jsonb_array_elements(state->'decisions') item) END,
    CASE WHEN jsonb_typeof(state->'open_questions')='array' THEN
      (SELECT string_agg(item #>> '{}', E'\n') FROM jsonb_array_elements(state->'open_questions') item) END,
    state->>'next_step')
$$;

CREATE OR REPLACE FUNCTION cronos_library_vector(config regconfig, title text, body text)
RETURNS tsvector LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT setweight(to_tsvector(config,cronos_library_fold(title)),'A') ||
         setweight(to_tsvector(config,cronos_library_fold(body)),'D')
$$;

CREATE INDEX IF NOT EXISTS messages_library_simple ON messages USING gin
  (cronos_library_vector('simple'::regconfig,'',cronos_library_message_body(content))) WHERE NOT excluded;
CREATE INDEX IF NOT EXISTS messages_library_russian ON messages USING gin
  (cronos_library_vector('russian'::regconfig,'',cronos_library_message_body(content))) WHERE NOT excluded;
CREATE INDEX IF NOT EXISTS artifacts_library_simple ON artifacts USING gin
  (cronos_library_vector('simple'::regconfig,filename,coalesce(extracted->>'text','')));
CREATE INDEX IF NOT EXISTS artifacts_library_russian ON artifacts USING gin
  (cronos_library_vector('russian'::regconfig,filename,coalesce(extracted->>'text','')));
CREATE INDEX IF NOT EXISTS projects_library_simple ON projects USING gin
  (cronos_library_vector('simple'::regconfig,name,cronos_library_project_body(name,goal,state))) WHERE NOT context_excluded;
CREATE INDEX IF NOT EXISTS projects_library_russian ON projects USING gin
  (cronos_library_vector('russian'::regconfig,name,cronos_library_project_body(name,goal,state))) WHERE NOT context_excluded;
