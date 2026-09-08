"""One-time adoption of the enabled default, preserving recorded user opt-outs."""

APPLY_DEFAULT_SQL = r"""
UPDATE users u SET
    proactivity_default_applied=true,
    preferences=CASE
        WHEN u.preferences->'proactivity'='false'::jsonb AND (
            EXISTS (
                SELECT 1 FROM operations choice
                WHERE choice.user_id=u.user_id AND choice.kind='home_preference'
                  AND choice.status='done' AND choice.result->'proactivity'='false'::jsonb
            )
            OR EXISTS (
                SELECT 1 FROM operations choice
                WHERE choice.user_id=u.user_id AND choice.kind='preferences_set'
                  AND choice.status='done' AND choice.result->'proactivity'='false'::jsonb
                  AND NOT choice.result ? 'error'
                  AND (
                      -- Without the original arguments, preserve an ambiguous opt-out.
                      NOT EXISTS (
                          SELECT 1 FROM operations model WHERE model.user_id=u.user_id
                          AND model.run_id=choice.run_id AND model.kind='model'
                      )
                      OR EXISTS (
                          SELECT 1 FROM operations model
                          CROSS JOIN LATERAL jsonb_array_elements(
                              CASE WHEN jsonb_typeof(model.result#>'{message,tool_calls}')='array'
                              THEN model.result#>'{message,tool_calls}' ELSE '[]'::jsonb END
                          ) call
                          WHERE model.user_id=u.user_id AND model.run_id=choice.run_id
                            AND model.kind='model'
                            AND call#>>'{function,name}'='preferences_set'
                            AND call#>>'{function,arguments}'
                                ~ '"proactivity"[[:space:]]*:[[:space:]]*false([[:space:]]*[,}]|$)'
                      )
                  )
            )
        ) THEN u.preferences
        ELSE u.preferences || '{"proactivity":true}'::jsonb
    END
WHERE NOT u.proactivity_default_applied
  AND ($1::bigint IS NULL OR u.user_id=$1)
  AND NOT EXISTS (
      SELECT 1 FROM privacy_requests request
      WHERE request.user_id=u.user_id AND request.state='erasing'
  )
"""
