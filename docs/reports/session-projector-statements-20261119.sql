-- Measured statement texts from the minimal Session prototype, reformatted.
-- Each PREPARE is independent. Substitute fixture parameters to EXECUTE it.

-- FRESH RUN: create/update the conversation's current Inbox.
-- $1 tenant, $2 physical Session, $3 native Inbox ID, $4 commit sequence,
-- $5 ordinal, $6 current items array, $7 Run, $8 writer/claim UUID.
PREPARE pma_inbox (uuid, uuid, bigint, bigint, integer, jsonb, uuid, uuid) AS
WITH owner AS MATERIALIZED (
    SELECT id FROM runs
    WHERE tenant_id = $1 AND session_id = $2 AND id = $7
      AND lease_id = $8 AND state = 'running'
    FOR SHARE
), changed AS (
    INSERT INTO inboxes
    SELECT $1, $2, 1, $3, $4, $5, $6::jsonb FROM owner
    ON CONFLICT (tenant_id, pi_session_id, conversation_id) DO UPDATE
       SET commit_seq = EXCLUDED.commit_seq,
           ordinal = EXCLUDED.ordinal,
           items = EXCLUDED.items
     WHERE inboxes.doc_id = EXCLUDED.doc_id
       AND (inboxes.commit_seq, inboxes.ordinal) < (EXCLUDED.commit_seq, EXCLUDED.ordinal)
    RETURNING doc_id
)
SELECT EXISTS (SELECT 1 FROM owner) AS accepted,
       (SELECT count(*)::int FROM changed) AS writes;

-- FRESH OR RETURNING: one complete user or assistant Entry.
-- $1 tenant, $2 physical Session, $3 Run, $4 commit sequence,
-- $5 entries with id/conversation/role/content/input/ordinal, $6 writer/claim UUID.
PREPARE pma_message (uuid, uuid, uuid, bigint, jsonb, uuid) AS
WITH owner AS MATERIALIZED (
    SELECT id FROM runs
    WHERE tenant_id = $1 AND session_id = $2 AND id = $3
      AND lease_id = $6 AND state = 'running'
    FOR SHARE
), changed AS (
    INSERT INTO messages (
        tenant_id, pi_session_id, entry_id, conversation_id, run_id,
        role, content, source_input_id, commit_seq, ordinal
    )
    SELECT $1, $2, v.id, v.conversation, $3, v.role, v.content, v.input, $4, v.ordinal
    FROM jsonb_to_recordset($5::jsonb) v(
        id bigint, conversation bigint, role text, content text, input uuid, ordinal int
    ) CROSS JOIN owner
    ON CONFLICT (tenant_id, pi_session_id, entry_id) DO UPDATE
       SET entry_id = EXCLUDED.entry_id
     WHERE (messages.run_id, messages.commit_seq, messages.ordinal,
            messages.role, messages.content, messages.source_input_id)
           IS NOT DISTINCT FROM
           (EXCLUDED.run_id, EXCLUDED.commit_seq, EXCLUDED.ordinal,
            EXCLUDED.role, EXCLUDED.content, EXCLUDED.source_input_id)
    RETURNING entry_id
)
SELECT EXISTS (SELECT 1 FROM owner) AS accepted,
       (SELECT count(*)::int FROM changed) AS writes;

-- FRESH OR RETURNING: PG confirms the independent work_finished record.
-- $1 tenant, $2 physical Session, $3 Run, $4 writer/claim UUID,
-- $5 stable history head (version, seq, nextId, floor, count, bytes).
PREPARE pma_finish (uuid, uuid, uuid, uuid, jsonb) AS
UPDATE runs
SET state = 'completed', settled_at = clock_timestamp(), history_head = $5::jsonb
WHERE tenant_id = $1 AND session_id = $2 AND id = $3
  AND lease_id = $4 AND state = 'running'
RETURNING id;

-- RETURNING ONLY: bootstrap the Projector for the successor's first commit.
-- $1 Run, $2 tenant, $3 physical Session.
PREPARE pma_open (uuid, uuid, uuid) AS
SELECT r.state, r.lease_id, r.mailbox_position,
       prior.history_head AS prior_head, i.doc_id AS inbox_id, i.items AS inbox_items
FROM runs r
LEFT JOIN runs prior ON r.mailbox_position > 1 AND prior.tenant_id = r.tenant_id
                    AND prior.session_id = r.session_id
                    AND prior.mailbox_position = r.mailbox_position - 1
                    AND prior.state = 'completed'
LEFT JOIN inboxes i ON prior.id IS NOT NULL AND i.tenant_id = r.tenant_id
                   AND i.pi_session_id = r.session_id AND i.conversation_id = 1
WHERE r.id = $1 AND r.tenant_id = $2 AND r.session_id = $3;
