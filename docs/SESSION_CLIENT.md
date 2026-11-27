# Session clients

The #52 candidate returns adopted inputs as the conversation tree's navigation entries. Each entry identifies its sender class, Turn and exact native Entry. The optional branch `answerAnchors` contains the sealed answer IDs used by Fork and prune actions. Before placement a human input appears in the editable queue. Stop returns unplaced human input to the composer and holds unread Agent mail until a human starts another Turn. [Candidate contract](adr/0369-placed-input-navigation.md).

`@pi-cloud/session-client` owns the existing public Session HTTP methods,
snapshot-first SSE, Session state reducer and uncertain-submission identity.
It is shared by the web application and non-browser clients under ADR-0321.
It has no DOM, React, Pi, database, Kafka or device execution dependency.

```ts
import { SessionApi, createInitialSessionView, sessionViewReducer } from "@pi-cloud/session-client";

const api = new SessionApi(fetch, accountToken, "https://agents.example.com");
const connection = new AbortController();
let state = createInitialSessionView();
await api.streamSession({
  sessionId,
  signal: connection.signal,
  onSnapshot: ({ conversation }) => {
    state = sessionViewReducer(state, { type: "conversation.loaded", conversation });
  },
  onEvent: (event) => {
    state = sessionViewReducer(state, { type: "stream.event", event });
  },
  onStatus: (status) => {
    // Display connection status using the application's own UI.
  },
});
```

The application supplies its existing authorized account token or authenticated
fetch implementation. A browser using same-origin cookies needs neither an
explicit origin nor a token. No token is persisted by this package. Abort the
connection signal to disconnect that client; explicitly call `cancelTurn` for
Stop. Disconnecting does not send a mutation or affect another subscriber.

The server joins PG history and the current Projector view. Every stream starts
with one replacement snapshot; clients receive subsequent events from that
point. Reconnect uses the same snapshot-first contract. Native storage and Kafka
positions are not public client responsibilities. Older history is retrieved
through `getConversation` with the existing bounded history query.

`ComposerSubmission` preserves an uncertain submission's idempotency key and
local feedback. Platform-local draft persistence, uploads, text selection,
scrolling and animations remain application responsibilities. Existing native
steer/follow-up, queue editing and returned-input APIs are shared without adding
a second queue. Run/Tool observations remain disposable stream data.

This package does not install a desktop application or expose a user's computer
for Tool execution. Cloud Workers retain the Agent loop; device transport is a
separate explicit integration tracked by #38.


## Independent Session placement receipts

The segmented queue snapshot includes latestRead, either null or the authenticated sender’s latest canonical input ID, Turn ID and initial-message flag. Initial queue items carry initialTurnId, which may differ from the stable input ID. The client reconciles snapshots using the monotonic queue version. Committed outstanding input is delivered. A placed canonical user Entry is read.

The Session SSE stream emits input.placed after accepted PG projection, with the Session, Turn, stable input ID, canonical Entry ID and commit sequence, initial flag, original text and upload metadata. Subscribers can render a follow-up immediately and refresh the sender-scoped queue snapshot. Reconnect recovers from canonical history and the queue. The observation uses the existing ordered SSE connection and does not advance its Kafka/display cursor. [Decision](adr/0376-direct-turn-claims-and-fork-locality.md).
