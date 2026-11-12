# Execution adapters

How a venue execution adapter works, what it must promise, and what a new venue has to
write. The reader is the engineer building venue N+1; everything here is enforced by the
fitness suite, the compiler, or a named convention — nothing by hope.

## Where an adapter sits

An execution adapter is an async edge actor. It owns no market or order state — the hot
thread owns all of that — and it cannot tell the engine anything except by pushing messages
onto its one input queue. Its whole job is translation with judgement: engine intents out to
the venue's wire, venue answers back into the engine's vocabulary, and honest bookkeeping
for the window where a request has left but no answer has landed.

The hot engine is deterministic: its state is a pure function of the ordered message
sequence it consumes, so a recorded tape replays to identical state. Everything an adapter
does must preserve that. The adapter may read clocks, sleep, retry, and reconnect — the hot
thread never sees any of it, only stamped messages in queue order.

## The two vocabularies

**Commands in** (one `rtrb` ring of `ExecLaneItem`, drained by `ExecCore::drain_commands`):
`Place`, `Cancel`, `AmendQty` (shrink only — growth is cancel+place), `ReconcileOrder`,
`ReconcileOpenOrders`, `CancelOurs`, `CancelPriorRun`. These are intents, not venue methods.
There is deliberately no cancel-all: the engine cannot distinguish foreign orders, so it
never asks for something it could not account for.

**Events out** (`InboundMessage::Exec(ExecEvent)` and `::Account(AccountChunk)` through
`EventFunnel`): sixteen `ExecKind`s covering acks, reports, refusals, snapshots and stream
lifecycle, plus balance chunks. Everything is a fixed-size POD of integer newtypes — prices
and quantities are i64 mantissas, identity is packed integers, and no string crosses inward.

Both live in `msg/exec.rs`. The venue's raw codes ride along as tape-only fields; the hot
engine branches solely on the normalised enums.

## The four tiers of the contract

1. **Types.** The vocabularies above, plus the classification enums: `RejectClass`
   (StillLive / Refused / Gone / Ambiguous / Fatal), `Provenance` (Mine / PriorRun /
   Foreign), `VenueOrderStatus` (eight spellings, `PendingCancel` deliberately
   non-terminal), `RejectVerdict` (an order verdict or a venue-availability verdict — an
   outage never feeds the reject streak).

2. **Shared machinery, embedded by composition.** An adapter may not re-implement:
   `ExecCore` (the phase machine: Down → Resyncing → Quoting → Cancelling → Settled, with
   fail-closed placement admission, and the fold of every venue event into its mirror),
   `OrderMirror` (the cancellability mirror — not a second OMS: no fills, no lifecycle),
   `ExecSession` (the apply layer every venue and the simulator compose: one core, one
   in-flight table, one resync pass, one event sink, and the rules for refusals, in-doubt
   marking, disconnect, expiry, resync polling and readiness arming), `EventSink` (where the
   session's synthesised events go: the venue's input queue, or a plain list in the simulator
   and the fitness suite), `InFlightTable` (request tracking; an expired request is
   reconciled, never re-sent), `ResyncPass` (numbered passes, bounded retries, give-up drops
   the connection), `ExecStop`/`EdgeHandle` (the stop latch and the one shutdown body), the
   synthesised-event constructors (every refused or timed-out command still answers the hot
   slot), and `VenueCapabilities`/`LeaseNamespace` (declared beside each other in every venue
   module). `ExecSession`, `EventSink` and `InFlightTable` are public on their own merit, as
   the shared machinery a venue embeds, with the same standing as `ExecCore` and `ResyncPass`.
   Driving commands through `ExecCore::on_command` and answers through `ExecSession::fold` is
   what makes the money rules inherited instead of re-derived.

3. **The session chassis.** `run_edge` (in `adapters/edge.rs`) owns the lifecycle every
   venue shares: connect/backoff with full jitter, the blind-too-long cancel sweep, offline
   settling, exit precedence (Shutdown > Fatal > Park), sweep pacing, and the stop
   protocol. A venue implements the ten-method `EdgeDriver` trait with only its own
   physics: its URL, its `select!` loop, its offline folding, its sweep step, its closing
   report. The same file holds what a venue's bring-up is handed and hands back:
   `EdgeInputs`, which carries `EdgeSettings` (the settings every edge shares, built once by
   the runtime), and `VenueEdge`. It also holds what a live venue builds its edge from:
   `EdgeControl::new`, the shared tick constants (`COMMAND_POLL`, `HOUSEKEEPING`,
   `RECONCILE_INTERVAL`, `RECENT_TRADES`), and the `EventSink` that feeds the venue's input
   queue. `EdgeHandle::spawn` gives a live venue its handle; the simulator builds its own,
   because a sweep it misses invalidates the run rather than leaving orders on a venue. The
   chassis file legitimately owns a clock; everything under `adapters/exec/` is clock-free by
   scanned law — deadlines there take `now` as a parameter.

4. **Written obligations** — the rules no signature can carry:
   - Every command is eventually answered. A refusal, a timeout, a disconnect — each
     synthesises an event; no hot slot ever waits forever.
   - One input queue per adapter, events stamped with monotone `queued_ts_us`. Acks and
     stream reports share that one queue so the tape is deterministic. Never split lanes.
   - Fill quantities are cumulative absolute venue totals, never deltas. A dropped or
     re-delivered event must be a no-op, which is what makes drop+count safe. A venue that
     reports per-fill increments gets accumulated at the edge, before the seam.
   - Balance chunks carry absolute totals and a venue-stamped, monotone
     `venue_update_ts_ms` that advances only when money moved. A delta stream is converted
     to snapshots at the edge; a lagging balance read is never presented as fresh.
   - Timeouts and ambiguity reconcile — a status probe resolves them; nothing is re-sent.
     Re-sending risks two live orders, which no reconciliation can undo.
   - `AckCanceled`/`ReportCanceled` are a finality promise: this order can no longer fill.
     A venue that cannot promise that reports `PendingCancel` and stays silent until it
     can. The hot side holds the slot and blocks the side until finality lands.
   - An open order the codec cannot decode is fatal, not skipped. Quoting against an
     incomplete mirror strands somebody's order.
   - An unmapped venue error code halts rather than retries. Unknown means unknown.
   - Readiness arms in fixed order — user stream, then a full balance snapshot, then an
     open-orders snapshot end per instrument — and until all three arm, every quote is
     refused as not-ready.

## The session lifecycle

Connect → subscribe the user stream → resync pass (the venue's own read set: account plus
open orders, paginated or per-instrument as the venue demands) → fold every open order into
the mirror, classifying provenance → cancel prior-run orders and wait for them to drain →
arm readiness → Quoting. On disconnect: mark every in-flight request ambiguous, emit
`StreamReset` (which invalidates every hot slot to Unknown and disarms readiness),
reconnect with backoff, resync again. On exit: plan a cancel sweep, retry it paced until
the mirror confirms every order gone or the deadline forces an abort, then report, settle
the latch, and stop.

## Identity and ownership

The engine's `ClientOrderId` is a bit-packed u64 — run nonce, slot index, generation — an
address, not a name. `TeTag` digests the (strategy, te) identity. Every order is Mine,
PriorRun or Foreign; foreign orders are never mirrored and never cancelled.

Two identity patterns exist, chosen by whether the venue echoes a client id:

- **Id on the wire** (Binance shape): the codec encodes (tag, id) into the venue's client
  order id format and recovers ownership by parsing what the venue echoes, through
  `OrderOwnership::of`. Stateless and restart-safe.
- **Venue-minted ids** (Polymarket shape): the venue names the order in its placement
  answer. The adapter keeps a fixed-capacity id index, holds stream frames that name
  unknown orders until the mapping lands, and decides adopt / defer / leave-alone by
  policy. An order is unattributable between send and answer — the adapter carries that
  window, never the engine.

Every venue declares a `LeaseNamespace` beside its capabilities. The execution lease locks
one live process per engine identity per host and advances a durable run nonce per
(venue, account). The nonce-file names are pinned byte-for-byte by fitness: renaming one
orphans real nonce history and could re-mint client ids a venue has already seen.

## VenueCapabilities

Venue physics, declared once in the venue's module, consumed by code that never names a
venue: `holds_reservations_until_settled` (when a reservation may release),
`fee_model` (which taker-fee curve the flatten planner prices), `rotates_markets` (whether
the quote window machinery is live), `base_asset_is_position` (whether a base-balance floor
would strand exits), `order_budget` (the venue's declared order-rate buckets, metered
hot-side: quotes are refused early under their own reject reason; a flatten is never
refused but always spends). Capabilities are startup constants. Nothing discovered mid-run
may change them — a tighter limit learned live is venue-availability parking, not a
capability change.

Amend capability is deliberately not here: it is a per-instrument registry stamp
(`max_num_order_amends`; zero means the venue has no amend and every shrink degrades to
cancel+place before any wire code runs).

## What a new venue writes

Everything venue-specific lives in `src/adapters/<venue>/exec/`. The registration cost
outside it is one arm per dispatch in `src/adapters/venue.rs` plus its `VenueError`
variant there, and a dependency-allowlist amendment if signing needs a new crate.

1. `mod.rs` — credential env-var names, `capabilities()`, `lease_namespace(...)`.
2. `handle.rs` — the setup and `bring_up(...) -> VenueEdge`, which builds the setup from
   the runtime's `EdgeInputs` and defers the `EdgeHandle::spawn` until the hot thread runs.
3. `actor/` — the `EdgeDriver` impl. The actor composes `ExecSession` and `EdgeControl` and
   writes only its transport, its resync read set, its answer handlers and the order of its
   housekeeping steps. Same filename for the same concept as the existing adapters.
4. `codec/` — the real work: encode intents, decode answers and stream frames into
   `ExecEvent`s, classify every documented error into a `RejectClass` (or into a
   `RejectVerdict` when the venue also reports its own availability, so an outage never
   reads as an order verdict), normalise statuses, parse money exactly. Convention surface:
   `EncodeContext`/`DecodeContext`, `encode_request`, `decode_response`, `classify_error`,
   `WireError::is_fatal`, a symbol-or-token table.
5. Signing module if the venue needs one. Sign vectors are minted from the vendor's own
   SDK, never from our implementation — crypto has no local failure mode.
6. `probe` + preflight — a read-only account report proving credentials, permissions,
   clocks and venue facts before any order-mutating code exists, and a startup gate that
   refuses a run the probe would have refused.
7. `fixtures/<venue>/exec/` — committed real venue payloads with a non-Rust generator, the
   independent statement of the venue's wire.
8. `tests/fitness/<venue>_exec_codec.rs` — goldens over those fixtures: the full
   kind/status table, the reject table plus an unknown-input-halts proptest, id round-trip
   or correlation-policy pins, exactly-one-snapshot-end, byte-exact signed bodies.
9. Registry stamps: scales, order caps, amend budget, order-rate buckets, written by the
   venue's startup scale preflight in `src/adapters/<venue>/preflight.rs`.

The simulator is Binance-shaped and stays that way; a new venue's determinism evidence is
its codec goldens plus a test double driven through its real codec, delivery-permutation
style (ack-first, report-first, ack-only, report-only, ambiguous, duplicated, reordered —
the seven ways two answers to one request arrive).

## What not to do

Do not re-implement tier-2 machinery, however small it looks. Do not give a venue a second
input queue. Do not forward deltas inward. Do not resend anything on timeout. Do not treat
an unknown reject as retryable. Do not let a comment claim a capability the record does not
carry. And do not share code with another adapter because it looks parallel — the existing
adapters keep transport tables, resync read-sets, housekeeping lists and reject inputs
deliberately unshared, because they differ in load-bearing ways. False sharing costs more
than duplication here.
