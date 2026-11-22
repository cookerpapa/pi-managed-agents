# ADR-0348. Keep confirmed child outcomes on the existing relation

The #52 candidate retains one current native outcome and its commit sequence on
each registered child relationship. The ordered Session Projector derives it
from Pi's creation/stop receipts, ordinary task lifecycle and input submissions.
Co-commit changes with the admitted native batch and partition offset, under
the same Run writer guard. A caller's stop request alone cannot confirm a stop.
A later input can make a previously idle child active again. Retain the original
input's answer Entry ID for the existing original-result wait contract. Bodies
remain in the candidate message store. The public status view interprets these
facts together with the root Run's state, including unknown interrupted work.

This change is scoped to the isolated candidate. Verify handoff, replay,
pre-creation cancellation, success and failure, reopening and original/latest
result selection. Keep the deployment gate on the new Session store.

While the Run is active, the fold tracks its child creation/caller references,
non-background tasks and pending submissions. Skip duplicate activity writes,
keep the native abort and failure distinctions, and allow the next input to reopen
the child. The moving checkpoint includes the fold. A sealed handoff can rebuild
it from native current records and existing registered child relationships. The
candidate view is selected by the candidate composition; the matched online
release still uses its original schema and readers.

The list reads its 160-character caption from the placed original input, and can
return the registered child's ID and state before that input exists. Preserving
a caption for a never-placed input needs a separate retention decision.
