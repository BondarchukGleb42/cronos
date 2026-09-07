CREATE TABLE IF NOT EXISTS runtime_state (
  key text PRIMARY KEY, value jsonb NOT NULL DEFAULT '{}', updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS service_leases (
  key text PRIMARY KEY, owner text NOT NULL, expires_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS users (
  user_id bigint PRIMARY KEY, preferences jsonb NOT NULL DEFAULT '{}',
  plan text NOT NULL DEFAULT 'FREE', balance_micro bigint NOT NULL DEFAULT 25000000,
  reserved_micro bigint NOT NULL DEFAULT 0, entitlement_micro bigint NOT NULL DEFAULT 25000000,
  period_start timestamptz NOT NULL DEFAULT (date_trunc('day',now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'),
  period_end timestamptz NOT NULL DEFAULT ((date_trunc('day',now() AT TIME ZONE 'UTC')+interval '1 day') AT TIME ZONE 'UTC'),
  topup_micro bigint NOT NULL DEFAULT 0, memory_revision integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE users ADD COLUMN IF NOT EXISTS pending_plan text;
ALTER TABLE users ADD COLUMN IF NOT EXISTS billing_anchor timestamptz;
ALTER TABLE users ADD COLUMN IF NOT EXISTS content_reset_at timestamptz;
CREATE TABLE IF NOT EXISTS conversations (
  id uuid PRIMARY KEY, user_id bigint NOT NULL REFERENCES users(user_id), chat_id bigint NOT NULL,
  thread_id bigint NOT NULL DEFAULT 0, title text NOT NULL DEFAULT '', revision integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(user_id,chat_id,thread_id)
);
ALTER TABLE conversations ADD COLUMN IF NOT EXISTS title_auto boolean NOT NULL DEFAULT true;
ALTER TABLE conversations ADD COLUMN IF NOT EXISTS title_message_count integer NOT NULL DEFAULT 0;
ALTER TABLE conversations ADD COLUMN IF NOT EXISTS title_update_id bigint NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS deleted_topics (
  user_id bigint NOT NULL, chat_id bigint NOT NULL, thread_id bigint NOT NULL,
  deleted_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(user_id,chat_id,thread_id)
);
CREATE TABLE IF NOT EXISTS privacy_requests (
  id uuid PRIMARY KEY, user_id bigint NOT NULL, scope text NOT NULL CHECK(scope IN ('all','chat')),
  conversation_id uuid, chat_id bigint NOT NULL, target_thread_id bigint NOT NULL DEFAULT 0,
  origin_thread_id bigint NOT NULL DEFAULT 0, run_id uuid, source_key text NOT NULL UNIQUE,
  state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','ready','erasing','done','cancelled')),
  expires_at timestamptz NOT NULL DEFAULT now()+interval '15 minutes',
  job jsonb NOT NULL DEFAULT '{}', created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS privacy_requests_owner ON privacy_requests(user_id,state);
CREATE TABLE IF NOT EXISTS messages (
  id bigserial PRIMARY KEY, user_id bigint NOT NULL REFERENCES users(user_id),
  conversation_id uuid NOT NULL REFERENCES conversations(id), role text NOT NULL,
  content jsonb NOT NULL, source_key text UNIQUE, excluded boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS messages_conversation ON messages(conversation_id,id DESC);
CREATE TABLE IF NOT EXISTS memory (
  id uuid PRIMARY KEY, user_id bigint NOT NULL REFERENCES users(user_id),
  content text NOT NULL, category text NOT NULL DEFAULT 'preference', source text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(user_id,content)
);
CREATE TABLE IF NOT EXISTS events (
  id uuid PRIMARY KEY, update_id bigint UNIQUE, kind text NOT NULL DEFAULT 'telegram', payload jsonb NOT NULL,
  state text NOT NULL DEFAULT 'pending', owner text, lease_until timestamptz,
  available_at timestamptz NOT NULL DEFAULT now(), notified_at timestamptz,
  attempts integer NOT NULL DEFAULT 0, error text, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS events_pending ON events(state,available_at);
CREATE TABLE IF NOT EXISTS runs (
  id uuid PRIMARY KEY, event_id uuid UNIQUE REFERENCES events(id), user_id bigint NOT NULL,
  conversation_id uuid NOT NULL REFERENCES conversations(id), status text NOT NULL DEFAULT 'running',
  cancel_requested boolean NOT NULL DEFAULT false, fence bigint NOT NULL DEFAULT 1,
  memory_revision integer NOT NULL DEFAULT 0, created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);
CREATE TABLE IF NOT EXISTS operations (
  id text PRIMARY KEY, user_id bigint NOT NULL, run_id uuid, kind text NOT NULL,
  result jsonb, status text NOT NULL DEFAULT 'started', created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS outbox (
  id bigserial PRIMARY KEY, user_id bigint NOT NULL, chat_id bigint NOT NULL, thread_id bigint NOT NULL DEFAULT 0,
  payload jsonb NOT NULL, dedupe_key text NOT NULL UNIQUE, state text NOT NULL DEFAULT 'pending',
  owner text, lease_until timestamptz, next_attempt_at timestamptz NOT NULL DEFAULT now(),
  attempts integer NOT NULL DEFAULT 0, telegram_ids jsonb, error text, created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS sent_at timestamptz;
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(state,next_attempt_at);
CREATE TABLE IF NOT EXISTS schedules (
  id uuid PRIMARY KEY, user_id bigint NOT NULL, conversation_id uuid NOT NULL REFERENCES conversations(id),
  chat_id bigint NOT NULL, thread_id bigint NOT NULL DEFAULT 0, due_at timestamptz NOT NULL,
  timezone text NOT NULL DEFAULT 'UTC', interval_seconds bigint,
  instruction text NOT NULL, fixed_text text NOT NULL, dynamic boolean NOT NULL DEFAULT false,
  proactive boolean NOT NULL DEFAULT false, state text NOT NULL DEFAULT 'active',
  revision integer NOT NULL DEFAULT 1, source_key text UNIQUE, last_sent_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS schedules_due ON schedules(state,due_at);
CREATE TABLE IF NOT EXISTS occurrences (
  id uuid PRIMARY KEY, schedule_id uuid NOT NULL REFERENCES schedules(id), revision integer NOT NULL,
  scheduled_at timestamptz NOT NULL, event_id uuid NOT NULL, state text NOT NULL DEFAULT 'pending',
  UNIQUE(schedule_id,revision,scheduled_at)
);
CREATE TABLE IF NOT EXISTS usage (
  operation_id text PRIMARY KEY, user_id bigint NOT NULL, run_id uuid, model text NOT NULL,
  prompt_tokens bigint NOT NULL DEFAULT 0, completion_tokens bigint NOT NULL DEFAULT 0,
  cost_micro bigint NOT NULL DEFAULT 0, raw jsonb NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS ledger (
  id bigserial PRIMARY KEY, operation_id text NOT NULL UNIQUE, user_id bigint NOT NULL,
  kind text NOT NULL, amount_micro bigint NOT NULL, description text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS reservations (
  id text PRIMARY KEY, user_id bigint NOT NULL, amount_micro bigint NOT NULL,
  status text NOT NULL DEFAULT 'reserved', created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS artifacts (
  id uuid PRIMARY KEY, user_id bigint NOT NULL, filename text NOT NULL, mime text NOT NULL,
  path text NOT NULL, source_file_id text, extracted jsonb NOT NULL DEFAULT '{}',
  size_bytes bigint NOT NULL DEFAULT 0, checksum text, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS run_metrics (
  run_id uuid PRIMARY KEY, user_id bigint NOT NULL, duration_ms bigint NOT NULL,
  cpu_seconds double precision NOT NULL, peak_rss_bytes bigint NOT NULL,
  meter_quality text NOT NULL DEFAULT 'estimated', created_at timestamptz NOT NULL DEFAULT now()
);
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['users','conversations','messages','memory','artifacts','usage','ledger'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
    IF NOT EXISTS(SELECT 1 FROM pg_policies WHERE tablename=t AND policyname='owner_access') THEN
      EXECUTE format('CREATE POLICY owner_access ON %I USING (user_id = nullif(current_setting(''app.user_id'',true),'''')::bigint) WITH CHECK (user_id = nullif(current_setting(''app.user_id'',true),'''')::bigint)',t);
    END IF;
  END LOOP;
END $$;
GRANT USAGE ON SCHEMA public TO cronos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO cronos_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO cronos_app;
