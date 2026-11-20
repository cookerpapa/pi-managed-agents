# Model metering and statements

[ADR-0297](adr/0297-model-metering-receipts-and-statements.md) adds an accounting
port beside native execution. The account menu's **Usage and costs** page shows model
usage and simulated amounts in its Models tab. Compute and Storage use separate
[resource statements](RESOURCE_BILLING.md); no tab collects payment. `/usage` shows complete provider-reported consumption on demand. Customer charges additionally require the existing eligibility and pricing evidence. `/status` shows native context usage and the model window.

## Flow and authority

The trusted Models HTTP adapter assigns a new invocation UUID and bounded
tenant/Session/Conversation/Run/purpose metadata to each actual request. The
Worker Model Gateway checks it against the active grant. Ordinary model context
also carries writer/fence/native-task identity; the image Tool caller retains
its separately attributed, currently unpriced media path.
This attribution is independent of the reusable model transport key. The provider
removes the private header before forwarding model work. Its v8 execution UUIDs
distinguish fallback/retry attempts within one invocation.

Before dispatch, the provider commits admission to `/data/model-metering.bolt` on
its existing volume. The pinned bbolt adapter captures attempt starts and normalized
usage synchronously, before CPA's asynchronous plugin queue. Only identities,
timing, model/tier, quality and quantities are retained; never model payloads,
credentials, upstream URLs, error bodies or image bytes.

Under [ADR-0298](adr/0298-independent-metering-receiver.md), the provider sends
batches of at most 128 receipts directly to the independent `metering` service at
`POST /internal/v1/model-metering`, authenticated with the dedicated `metering-ingest-token`
key. Acknowledgement follows the PG transaction. Lost acknowledgements resend the
same identities; management availability is not required. Duplicate contents do not add consumption. A changed identity
or payload conflicts and leaves the source receipts pending for investigation.
Neither receiver downtime nor accounting recovery retries a model or Tool.

PG owns `model_invocations`, immutable `model_metering_events`, and
`model_rate_versions`. These have no cascading relationship to chat/native history.
Deleting a chat, Compaction and Fork do not erase or copy actual consumption.
The only cleanup exception is the explicitly owned acceptance-fixture purger.

Under [ADR-0330](adr/0330-model-charge-eligibility.md), model-task settlement
evidence accompanies the same native Kafka commit as Pi's state transition.
Projector inserts an immutable `execution` accounting event only when that batch
is accepted. One `model_execution_writers` closure fact retains each writer's
ordered cutoff independently of Session deletion. Neither fact grants execution.
The independent accounting view `model_billing_events` joins these exact identities
to raw usage and source completion. It does not poll Worker leases or give CPA a
live fencing cache. Either source may arrive first; missing evidence remains pending.

Customer eligibility is separate from actual usage. Completed work and valid
native user cancellation are chargeable with complete usage. Failed/superseded
work, a closed writer without accepted settlement, and ambiguous multi-attempt
provider attribution are platform cost. A valid pre-seal settlement remains
eligible when its usage arrives after replacement. No token count is split using
a seal timestamp. The statement shows actual quantities separately from billable
tokens and masks customer amounts without changing raw receipts or tariff quotes.

## Missing observations and limits

- A live invocation is pending. A started attempt without usage, interrupted
  admission, empty synthesized usage or incomplete token classification remains
  unconfirmed. It is not silently free or charged from an estimate.
- Known receipts remain useful even when another attempt is unconfirmed; a partial
  statement must not be mistaken for a complete provider invoice.
- Receiver downtime uses the local buffer. The default 64 MiB admission watermark
  reserves space for in-flight completions; admitted receipts are not dropped to
  regain capacity. New model work receives a pre-dispatch 503 when it cannot be
  recorded. ACK cleanup restores capacity. In-flight completion writes may exceed
  the admission watermark.
- Provider restart marks unfinished admissions interrupted and sends their existing
  receipts. A disk loss can destroy that single-host buffer. No implementation can
  reconstruct usage the provider never reported or a response lost before capture.
- An I/O failure after dispatch is logged without changing the model's result or
  causing replay. Already saved starts identify missing receipts. Restoring local
  writes permits new admission; it does not manufacture the missing usage.
- Delivery and metering failures log identifiers and bounded classifications only.
  The local journal is financial metadata, not a substitute for PG backup or an
  independent balance database.

## Prices

The platform operator creates immutable rate versions for an account, model and
service tier. Rates have a future effective time, currency and decimal prices
**per million tokens**. `customer` is a simulated customer
quote; `upstream_estimate` is a separate optional estimate. Neither is copied from
the other or inferred from a subscription. No real rates are seeded automatically.

Input, cache read, cache write, non-reasoning output and reasoning are disjoint
buckets from CPA v8's canonical accounting. Prices accept up to six decimal places
per million tokens; amounts are stored as exact integer trillionths of the currency
(`amountPico`). Summation uses integer/numeric arithmetic, with no per-token cent
rounding. A receipt permanently retains the rate selected at its actual attempt
start time. Later rate changes and duplicate receipt delivery do not rewrite it.

Image generation, hosted search and other unsupported media dimensions remain
unpriced even when token counts are available. Ordinary vision tokens can be
counted when the provider supplies canonical quantities. Incomplete quantities
remain unconfirmed. `includeFailed` is accepted only for `upstream_estimate` rates.
Customer failure/Stop handling follows native execution eligibility, not the
provider's failure flag: a valid user Stop with complete usage remains chargeable.

## API

All public routes use existing tenant authentication. Only the platform owner can
select another `tenantId` or create rates. Reads do not require the original chat
or Run to exist.

- `GET /v1/model-billing`: filters `from` (inclusive), `to` (exclusive), optional
  `sessionId`/`model`. Pages contain up to 200 invocations (default 50); the returned
  `next.time`/`next.id` become `beforeTime`/`beforeId`.
- `GET /v1/model-billing/summary`: the same filters, totals grouped by pricing
  kind/state/currency. Never sum different currencies into one number.
- `GET /v1/model-billing/export`: streams the selected range as NDJSON using bounded
  DB pages. The Web page also offers an explicit current-page export.
- `GET /v1/model-billing/rates`: the latest 500 configured versions for the account.
- `POST /v1/model-billing/rates?tenantId=...`: platform operator only. Body:

```json
{
  "kind": "customer",
  "provider": "openai-codex",
  "model": "example-model",
  "serviceTier": "default",
  "effectiveAt": "2099-01-01T00:00:00.000Z",
  "currency": "USD",
  "perMillion": {
    "input": "1.25",
    "cacheRead": "0.125",
    "cacheWrite": "2.5",
    "output": "5",
    "reasoning": "5"
  }
}
```

This is a fixture, not a published model tariff. Match the model/tier actually
reported by the provider. Backdated new changes are rejected. An identical retry, including after its
effective time, returns the existing version; conflicting reuse is rejected. There is no edit/delete
route for a historical rate or receipt. Ordinary generation and compaction use
the same eligibility boundary. Unsupported media/search pricing, payments,
wallets and BYOK collection require separate business decisions before collection.

## Publication

The independent receiver and resource-accounting topology are deployed on schema
212; see the [matched delivery](reports/runtime-accounting-20261107.md).
Protocol, storage and ownership changes use the maintained full matched procedure,
including fourteen custom artifacts, separately pinned native dependencies,
writer closure and confirmed compute retirement with Workspace preservation.
The historical schema-209→210 retained-runtime exception has been removed.
Supported provider/catalog/Web/ordinary-tool changes use their documented scoped
release paths; see [publication procedures](COMPONENT_RELEASE.md).

The provider needs `PI_CLOUD_METERING_PATH`, `PI_CLOUD_METERING_URL` and
`PI_CLOUD_METERING_KEY_FILE`; the receiver uses `DATABASE_URL_FILE` and
`PI_CLOUD_METERING_TOKEN_FILE`. It shares the host image but has a separate
entry point, health probes and Deployment. The management API has no receipt route.
Maintained Helm templates supply these from the dedicated receipt credential.
Delivery bypasses the public egress proxy. A missing/failed source journal refuses
attributed model work before dispatch. Ordinary non-PiCloud CPA callers have no
cloud attribution and do not enter the tenant statement.

The owned acceptance drivers are:

```sh
node scripts/run-model-billing-browser-check.mjs
PI_CLOUD_LIVE_MODEL_METERING_CHECK=1 node --import tsx scripts/run-live-model-metering-check.mjs /private/metering.json
PI_CLOUD_LIVE_MODEL_CONTINUATION_CHECK=1 node --import tsx scripts/run-live-model-continuation-check.mjs /private/continuation
PI_CLOUD_LIVE_FAST_COMPACTION_CHECK=1 node --import tsx scripts/run-live-fast-compaction-check.mjs /private/compaction.json
```

The first live driver requires no unrelated active Runs, temporarily stops the
Control Plane and confirms one owned Luna request is already recorded in PG.
It then separately stops the receiver, performs another owned request, replaces
the provider, restores the receiver before management, and verifies the receipt, explicit fixture tariff,
duplicate delivery, tenant API/export and browser. It restores services on errors.
The continuation driver verifies ordinary Worker/Cube requests from two owners
retain their own Run attribution. Fixture financial rows are purged only through
the restricted owned-fixture cleanup path.

The compaction driver uses an owned tenant and native in-memory history with a
small fixture window. Its real provider receipts distinguish maintenance from
subsequent generation; it does not alter production model limits or claim a
large-context benchmark.

## Receipt completion and maintenance

HTTP response completion does not necessarily end a provider's usage publisher.
The source adapter retains the existing request admission until the shared
executor stream and any supported post-stream usage publisher have both settled.
Client cancellation still returns immediately; the observer drains the existing
producer without retrying model work. Process loss follows the existing
interrupted-admission recovery, preserving uncertainty rather than inventing usage.
The optional SDK capture lifetime port and the pinned post-EOF publisher contract
are checked during provider upgrades.

The private `/internal/picloud/metering/delivery` endpoint reports only bounded
source delivery state and requires the receipt credential. Disposable acceptance
accounts are purged only after physical cleanup and verified source delivery;
ordinary account/history operations do not delete financial records. Compute
collection and Workspace closure have their own delivery evidence. These checks
never authorize VM destruction or replace a Session execution lease.
