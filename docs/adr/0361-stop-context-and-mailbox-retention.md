# ADR-0361. Bash Stop context and seven-day reply retention

The #52 source candidate keeps Pi's original aborted Tool Result. Its next model context explains that the agent stopped waiting and the remote outcome is unconfirmed. Only an acknowledged cancellation for a user Stop establishes that the platform requested cancellation. Reuse Pi's public context edits after settlement and during cold preparation. Keep partial output and the original call identity. A Stop does not initiate another request.

Kafka owns reply record expiry using seven-day delete retention and broker append time. The platform checks once a day for empty mailboxes belonging to boots whose retirement completed over seven days ago. Session log retention keeps its own policy. Remove the PG pending-operation minimum and its topic index from reply collection. Keep the immutable original reply coordinates on the intent so a cold dispatcher can verify its destination. Handoff readers use their original operation identity within the retained suffix. After catching up, a matching retained result wins. An unresolved operation whose required prefix expired settles as unknown, without issuing another execution. The original deadline still applies to an intact suffix.

This change runs in the isolated storage candidate. At matched cutover, drain old writers and collectors and apply the same Kafka topic configuration to previously created reply mailboxes before starting the new policy. No serving data or topic is changed by the experiment.

## Existing mailbox configuration at matched cutover

New Worker boots create the topic with the target policy. Kafka does not change an existing topic's explicit overrides when the broker default changes. After the drain, use the stock Kafka CLI on the target cluster. List only generated mailbox names, then set the two topic overrides. The commands can be repeated. Session log topics have separate retention rules. [Kafka topic configuration](https://kafka.apache.org/41/configuration/topic-configs/)

```bash
set -euo pipefail
mailboxes=$(kafka-topics.sh --bootstrap-server "$BROKERS" --list)
while IFS= read -r topic; do
  [[ $topic =~ ^pi-cloud\.tool-replies\.v4\.[[:xdigit:]]{8}(-[[:xdigit:]]{4}){3}-[[:xdigit:]]{12}$ ]] || continue
  kafka-configs.sh --bootstrap-server "$BROKERS" --alter --entity-type topics --entity-name "$topic" \
    --add-config 'retention.ms=604800000,message.timestamp.type=LogAppendTime'
done <<< "$mailboxes"
```

## Candidate verification

The real Runner Stop test reads the interrupted conversation through a freshly opened Pi Harness. It checks the original tool call ID, one model-facing result and no additional provider call. The real RF3 recovery test checks seven-day topic configuration, a truncated mixed mailbox, a matching retained result, a subsequent live result and a missing topic. Nine Bash handoff boundaries and the combined and append-layout history readers passed.

An isolated RF3/PG check also published exactly one reply and then stopped writing. After verifying the seven-day configuration, it shortened that test topic's retention to seven seconds and the broker cleanup check to one second. Its low watermark reached the end without another append. The collector preserved the nonempty retired mailbox on its first pass and deleted the empty topic on the next eligible pass. Kafka metadata reflected the deletion asynchronously.
