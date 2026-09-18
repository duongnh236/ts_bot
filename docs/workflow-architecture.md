# Android workflow boundaries (v124)

## Modules

- `workflows/train.py`: train command setup, party verification, channel capacity
  fallback, channel map scope, member catch-up and leader recovery.
- `workflows/digioi.py`: the all-account DG-to-train handoff.
- `workflows/daily.py`: independent daily city preparation and daily command setup.
- `workflows/common.py`: existing stop-combat/leave-current-area safety primitive.
- `workflows/lifecycle.py`: fresh session state, revision and cancellation per party.

The legacy runner retains public function names as compatibility adapters. UI callers
do not need to change. Implementations receive explicit services; they must not import
the runner, install module globals or reach into another workflow's implementation.
Train/Daily start fresh sessions. DG starts its session at the Android entry point;
its background handoff respects session cancellation as well as the command generation.

## Adding a workflow

1. Add a separate module and an explicit entry point; do not add mode branches to an
   existing workflow just to make a new feature run.
2. Allocate a new session via the lifecycle adapter. Keep feature-specific state in
   `session.state`; never reuse another feature's field names/events.
3. Check the session cancellation and command generation after waits/network actions
   before publishing progress, invitations or another command.
4. Share protocol/connection/navigation primitives, not workflow decisions. Reuse saved
   combat settings without modifying them as part of a flow transition.
5. Add tests for the new flow, old-flow regressions, cancellation and party independence.

## Deliberate migration boundary

This is a staged refactor, not a complete rewrite of the ~14k-line legacy runner.
The per-account command executor, native DG startup and some legacy party fields/events
remain there, and therefore some state is still shared through the compatibility adapter.
Do not assume full isolation or zero future conflicts. Moving those remaining sections
requires separate behavior-preserving changes and live route/packet validation.
Packet handling and Ground navigation are unchanged in this refactor.

Run `python3 -B -m unittest discover -s tests -q` before building a release.
