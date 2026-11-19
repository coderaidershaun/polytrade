# Polysim Constitution

Permanent engineering law for polysim market-data + quant engine. All docs, modules, and code
comply. IF one cannot comply, THEN surface conflict explicitly. Amendments: §15.

## Words

| Word                | Meaning                                                                      |
| ------------------- | ---------------------------------------------------------------------------- |
| hot thread          | ONE synchronous hot-path thread that owns all market state (§2)              |
| edge actor          | tokio async actor at I/O edge (§2)                                           |
| lib                 | a lib crate of the workspace: `engine` or `lab` (§8). "The lib" = `engine`   |
| bin                 | binary                                                                       |
| TE                  | trading engine (§8)                                                          |
| fitness             | test target `fitness` of a lib crate: `engine/tests/fitness/` (§12)          |
| gate G              | the six gate steps of §14                                                    |
| live policy         | rules of the owner for keys and live runs. Git does not track them (§12)     |
| `live-runner`       | the one role of a job that starts a live run, by the live policy (§12)       |
| failure class       | the reaction to an error: HALT, RESET, RETRY, or REJECT (§6)                 |
| quoting halt        | quoting stops, and the process stays up: `HaltReason` (§6)                   |
| engine halt         | fatal latch, bounded drain, exit code not 0: `FatalReason` (§6)              |
| ring-pressure halt  | a quoting halt that follows ring space, not messages (§6)                    |
| incident record     | typed record of a money event, with a count, in its own table (§6)           |
| marker              | the message that carries a desired run state to the fold: `RunControl` (§6)  |
| controller          | a workstation that asserts a run state over the link (§6)                    |
| polysim             | the repo that this workspace came from. In a clause: the code of this repo   |
| Amended `<date>`    | amendment of that date, with its WHY (§15)                                   |
| Replaced, WITHDRAWN | record of earlier law, not in force                                          |

## 1. Prime Directives

- [ ] Simple beats clever. IF straightforward design meets requirements without heavy performance
  loss, THEN it wins. No exceptions for interesting ideas.
- [ ] Abstraction must pay rent at call sites. Deletion test: remove module. IF complexity
  vanishes, THEN module = pass-through. Delete it.
- [ ] One design language. Same problem = same solution everywhere.
- [ ] Keep comments to a minimum. Prefer ergonomics over comments. `rex-code-commenting` binds each
  comment (§10, 2026-11-12 section).
  - Replaced 2026-11-12: "Use /rex-utils-caveman skill for commenting."
- [ ] Aim high, ship lean. 80% value from 20% code.
- [ ] No early factoring. Cut points emerge first. Then trap complexity behind narrow interface.
- [ ] No source file > 500 lines.
  - Amended 2026-07-21: files under strategies/ = exempt. WHY: strategy implementations are
    one-file customer code. A forced split serves no reader.
  - Amended 2026-07-22: cap counts production lines only. `#[cfg(test)]` blocks excluded. WHY: cap
    buys navigable production logic. Inline pins appended at bottom obstruct no reader, and §12
    invites them on quant calculators. To count them taxes testing.
  - Amended 2026-07-26: cap counts production lines under `<package>/src/` and in binaries. Files
    under `<package>/tests/` = exempt. WHY: a file under `<package>/tests/` is ENTIRELY test code.
    It sits in no `#[cfg(test)]` block, because a test target needs none. The 2026-07-22
    carve-out's own logic reaches it.

## 2. Execution Model

- [ ] Two domains, permanent: tokio async actors at I/O edge + ONE synchronous hot-path thread
  that owns all market state. Nothing else touches that state.
- [ ] Hot path: no async, no locks, no shared mutable state, no blocking syscalls in steady state.
- [ ] All communication with hot path goes through fixed-capacity SPSC rings (`rtrb`).
- [ ] Hard max: 20 input queues.
- [ ] Single-writer: every piece of state has exactly one owning thread.
- [ ] CPU pinning required. Hot thread = pinned. Other affinity = design decision, not afterthought.
- [ ] Hot-path state = pure function of ordered input message sequence.
- [ ] Time-driven behavior (spin ticks) arrives as messages from async timer actor.
- [ ] Wall clock never drives state transition on hot thread. Clock reads there only stamp latency
  metrics.
- [ ] Consequence: replay of recorded input sequence gives identical hot-path state. This is basis
  of fitness tests + future backtesting. Never break it.

### Amended 2026-07-29: edge actor can synthesize a venue

M13 `exchange-sim`. Ratified O1.

- [ ] Edge-actor rule governs WHERE STATE LIVES, not whether a socket exists.
- [ ] An edge actor can SYNTHESIZE a venue instead of reaching one.
- Determinism above is untouched, and it is the reason this is safe. Output of a synthesizing
  actor is ordinary `InboundMessage` traffic on an ordinary input queue. Thus replay of a recorded
  tape reproduces hot state exactly. Hot thread cannot tell which side of the socket its input
  came from.
- [ ] Binding consequence: such an actor is still an EDGE actor and owns no hot state.
- [ ] Binding consequence: its own decisions must be a pure function of stamped input messages.
  IF it reads a clock to decide what to emit, THEN it relocates the nondeterminism. It does not
  remove it.

### Amended 2026-11-13: preflight, then the start in pin order

WHY: a start that was refused after its threads ran left threads running, spent a run nonce, and
could prune tape files. A hot thread that could not pin tripped the fatal latch after each thread
had started.

Preflight:

- [ ] `Preflight { identity, config, strategy }` runs each start check before any thread starts:
  the registry (with the cap of 20 input queues), the core id, the scale read, the order capacity,
  the exposure file, the permission probe, the link names and the link bind, the execution checks,
  and the checks of the venue setup.
  - Amended 2026-11-18: `Preflight` has a fourth field, `locks: &IdentityLocks`. After the link
    bind and before the execution checks, preflight takes the identity lock of the engine, in each
    mode, `off` included (`engine/src/runtime/assembly/preflight.rs`). A held lock refuses the
    start with `StartError::ExecutionIdentityInUse { path }`, which names the lock file. The engine
    holds the lock for its whole run, and the drain lets it go last. WHY: two engines of one
    identity share the files of the run, the tape folder included, and the tape writer removes each
    unfinished file of another boot (§6, 2026-11-17 section on tape files).
  - The lock file is `<folder>/.exec-<TeTag of the identity, 8 hex digits>.lock`, in the folder
    `polysim-execution-locks` below the temporary folder of the OS. The identity alone names it.
- [ ] The execution lease is the last check. So each other refusal leaves the run nonce unspent.
  - Amended 2026-11-18: a lease holds no lock. `ExecutionLease` advances and holds the run nonce of
    its venue only, as the last step of an armed preflight. The identity lock comes before it, and
    the checks of the execution setup run between the two. Thus a refused identity lock or a
    refused execution check leaves each nonce unspent. Section on several sources, end of §8.
  - Replaced 2026-11-18: "Amended 2026-11-13: each venue has its own lease. Preflight takes the
    lock of each lease before it advances any nonce. Section on several sources, end of §8." WHY:
    the lock left the lease and became the identity lock of the engine, in each mode.
- [ ] The venue reads go through one `VenuePort`. `Preflight::run` uses the real port, which runs
  them on one current-thread runtime and drops it before it returns. `Preflight::run_with(port)`
  takes a stand-in, so a test needs no key.
- [ ] A refused check returns a `StartError` (HALT, start refused). Preflight writes no file and
  starts no thread. The binary prints the error, and the exit is `Exit::StartRefused`.
- [ ] The link socket is bound in preflight and held in the `StartPlan`. A drop of the plan closes
  it, so a refusal needs no teardown.
- [ ] Preflight runs before the log exists. It hands what it found to the start, which logs it.

The start:

- [ ] `Engine::start(config, strategy, plan, pin)` starts what the plan checked. Each thread that
  can refuse its pin starts first, in this order: the hot thread, the log, the metrics thread, and
  the thread of the pinned senders. The pin is the first statement of each.
- [ ] Then the engine runtime and the writers start. The hot thread builds its input rings and its
  engine on its own core, and touches that memory. Then the timer, the execution edge, the
  market-data actors, and the link start.
  - Amended 2026-11-18: the tape writer starts after the hot thread hands back the producers of
    its input rings, because it takes the producer of its own status queue (§6, 2026-11-18 section
    on tape faults). The exposure writer starts after it, then the execution edge, the timer, the
    market-data actors, and the link. The record ring of the tape is made before the hot thread.
- [ ] IF a pin is refused (Linux), THEN each thread that started stops before the start returns.
  No writer has started, so no tape file is pruned. The run nonce is spent by then.
- [ ] The pin call is a parameter (`PinCall`). `run_trading_engine` passes `pin_to_core`, and a
  test passes a stand-in.

### Amended 2026-11-13: the OTLP check and the exporter

WHY: the export adds one start check and one thread, and neither may change the order of the pins.
The stop of the exporter must end before the watchdog of the drain, after the exit sweep.

- [ ] Preflight checks the `otlp:` block (feature `otlp`) after the core id and before the scale
  read: a collector on a loopback address (`127.0.0.0/8` or `[::1]`), and room for the stop of the
  exporter. It starts nothing, and it reads no network.
- [ ] The watchdog of the drain starts with `engine.drain_deadline_ms`, and the exit sweep can take
  its share first: `engine.drain_deadline_ms / SWEEP_SHARE_OF_DRAIN`, rounded up, half today. So
  preflight refuses an `otlp:` block unless `logging.drain_deadline_ms` + 1,000 is below the rest.
  The 1,000 ms are the stop of the exporter. The default deadlines (5,000 and 2,000) do not fit:
  a config that exports sets a longer `engine.drain_deadline_ms` or a shorter
  `logging.drain_deadline_ms`.
- [ ] The exporter thread does not pin, so it is not in the pin order. Its lanes open right after
  the log starts. The thread starts last, after the link, when no refusal can follow. So no refused
  start has an exporter to stop.
- [ ] IF the OS refuses the exporter thread, THEN the run goes on with no export and one ERROR line.
  The start never panics for the export, so the drain and the exit sweep always run.

### Amended 2026-11-18: the hot loop folds each message pushed before a drain

WHY: a hot thread that found its queues empty, and then saw the drain request, stopped. A message
that its producer pushed between the two reads was never folded, and then the replay differed
from the run at its end.

- [ ] IF a step of the hot loop finds each queue empty and the drain is requested, THEN the loop
  steps one time more. It stops only IF that step also finds each queue empty. ELSE it goes on
  (`HotLoop::run`, `engine/src/hot/spawn.rs`).
- [ ] A tripped fatal latch still stops the loop at once, with no further step.
- [ ] The live drain stops the producers of the edges first, then requests the drain, then joins
  the hot thread. Thus each message that a producer pushed before the request is folded.
- [ ] No test pins the order alone. The replay test
  `two_threaded_runs_give_equal_tables_and_drain_clean` (`engine/tests/fitness/tape/e2e_replay.rs`)
  requests the drain right after its pushes. It failed 1 run of 20 under load before the change,
  and 0 of 20 after it.

## 3. Memory

- [ ] Fixed capacity wherever practical: preallocated at startup, contiguous, predictable layout,
  cache-friendly access.
- [ ] Heap = initialization only.
- [ ] Steady-state hot path: zero alloc, zero dealloc, zero resize. Counting-allocator fitness test
  enforces this, not trust.
- [ ] Capacity hit = designed event with explicit per-structure policy. Never silent growth.
- [ ] Memory is cheap. Latency variance is not. Trade memory for simplicity + predictable movement.
  Canonical example: FastQueue oversized backing allocation.
- [ ] No strings in hot-path lookups. Identity = compact typed indices, assigned from config at
  startup. String-like types live at edges.

### Amended 2026-11-13: capacity from the registry

WHY: constants capped the instruments and the assets at 16. The start refused 17 instruments, and
asset 17 read 0 with no error. The registry knows each count at start.

- [ ] No constant caps the count of instruments or of assets. A table whose size follows one of
  these counts takes its size from the registry rows at start. Among them: order slots, prior-run
  slots, account rows, the refusal latch, the command bank, the exposure snapshot.
- [ ] `ExecCapacity` holds the size of each execution table. The start computes it one time from
  the instrument rows. A run, the timing harness, and each test size an engine through it, from the
  same rows.
- [ ] The start refuses a count only where the bits of the client order id end. The slot bits must
  address each slot of the order table. Thus 16 slot bits and 64 slots for each instrument give
  at most 1,024 instruments.
- [ ] That refusal is a HALT (start refused) of preflight, before any thread starts (§2,
  2026-11-13). It names the table, the count that the table needs, and the count that the bits
  address.
- [ ] The start makes each table one time, at its full size. The rules at the top of §3 hold for
  it: no growth, and a designed policy for a full table.

## 4. Numerics

- [ ] Prices and quantities cross adapter boundary exactly once. There, exchange decimal strings
  become `i64` fixed-point mantissas. Global scale is fixed during design.
- [ ] Book keys = exact integers: exact equality, total ordering, hashable.
- [ ] `f64` only downstream of exact state: derived features, statistics, model math.
- [ ] Float is never: a key, a money accumulator, an equality operand in book logic.

### Amended 2026-11-13: money types

WHY: quote money was a bare `i64`, and one field held quote money for a buy and base quantity for a
sell. A swap of the two units compiled.

- [ ] `Price`, `Qty`, and `Notional` (quote money) keep their mantissa private, at `FIXED_SCALE`.
  `from_mantissa` and `mantissa` are the only ways in and out.
- [ ] No operator and no conversion leads from a `Qty` or a `Price` to a `Notional`. Only
  `Price::notional(qty)` makes quote money from a trade. Its product is exact in `i128`, and a
  result outside the `i64` mantissa panics with the price and the quantity.
- [ ] What an order reserves is a `Spend`: `Quote(Notional)` for a buy, `Base(Qty)` for a sell.
  `Side::spend` gives it.
- [ ] An amount in an asset that a field beside it names (a commission, a row of balances) stays an
  `i64` with that asset.
- [ ] A market statistic in quote units (the quote volume of a candle, the notional of sampled
  prints) stays a plain number. It never reaches the order path or the ledger.
- [ ] A rate (the taker fee rate, a distance in hundredths of a basis point) stays an `i64` at its
  own scale. It is not money.

## 5. Time

- [ ] µs resolution everywhere.
- [ ] Every timestamp field ends `_ts_us` (`exchange_ts_us`, `received_ts_us`, `queued_ts_us`,
  `processed_ts_us`).
- [ ] Timestamps and durations = typed wrappers with ergonomic ops (`a.diff(b)`).
- [ ] Bare `i64` never crosses an API.
- [ ] Exact type set is fixed in design.
- [ ] Exchange time ≠ local receive time ≠ monotonic latency clock. Never interchange them
  silently.

### Amended 2026-10-14: MCP wire timestamps

WHY: the MCP wire is an external JSON contract read by agents and a UI, so its timestamps are
civil strings, and every internal field stays a stamp type with the `_ts_us` suffix.

- [ ] A timestamp on the MCP wire is an RFC3339 UTC string with a µs fraction, for example
  `started_at: "2026-10-14T19:05:03.123456Z"`.
- [ ] It is formatted from a `LocalTs` or a `VenueTs` at the wire edge, by the crate's own
  civil-time conversion. No calendar crate.
- [ ] Internal fields stay a `LocalTs` or a `VenueTs` with the `_ts_us` suffix.
- Replaced 2026-11-13: "every internal field stays `TsUs`/`_ts_us`", "It is formatted from a `TsUs`
  at the wire edge", and "Internal fields stay `TsUs` with the `_ts_us` suffix." WHY: one stamp
  type became two clock domains (2026-11-13 section below).
- [ ] A duration on the MCP wire is an integer whose field name ends in its unit (`duration_ms`),
  while every internal duration stays `DurationUs`.
- [ ] Scope: the MCP wire ONLY. Every other API, record and file keeps the rules above.

### Amended 2026-11-13: two clock domains and the fold time

WHY: one stamp type served each clock, so a span from a venue stamp to a local stamp compiled with
no sign. A tick stamped at its slot can pop after a newer message, so a deadline could read an
earlier time than the one before.

- [ ] Two stamp types, each in µs since the Unix epoch:
  - `VenueTs`: a stamp that this process did not read from its own clock: a venue clock, a venue
    calendar (a window or a candle grid), a peer engine, and, in the workstation, each stamp of
    the feed.
  - `LocalTs`: a reading of this process's `EngineClock`, and each time computed from such
    readings: scheduled spin slots, deadlines, and the times of the simulator.
- [ ] A `DurationUs` comes only from two stamps of one type (`a.diff(b)`). No operator takes a
  `VenueTs` and a `LocalTs` together.
- [ ] A span or a comparison across the two domains goes through one named method, so its
  assumption stays visible at each call: `VenueTs::offset_from(local)`,
  `LocalTs::on_venue_clock(offset)`, `LocalTs::as_venue_calendar()` (the host clock read as the
  venue calendar, or the stand-in for a venue stamp that is missing), and `LocalTs::to_peer()` (a
  stamp of this process as another process reads it).
- [ ] A stamp that crosses to another process (a link frame, a UI event, the latency report) has
  the type of its reader: `VenueTs`. The writer converts it one time, with `to_peer()`.
- [ ] `FoldTime` is the time of the fold: the latest received stamp of the messages folded so far.
  It never decreases. Deadlines, warmup, and the time of the strategy context (`ctx.now()`) read
  it.
- [ ] The raw received stamp of a message stays on the message, on each tape row, and on each log
  record (`ctx.event_ts()`), so a replay writes the same rows.
- [ ] `wall_clock_now()` reads the wall clock of this process, for a stamp that never becomes
  engine state: the link boot stamp, and the dates of logs and files. It returns a `LocalTs`, and
  it can step.

### Amended 2026-11-13: OTLP wire timestamps

WHY: the OTLP wire is an outside contract that a collector reads, and it counts time in nanoseconds.

- [ ] A timestamp on the OTLP wire is a decimal string of nanoseconds since the Unix epoch, for
  example `timeUnixNano: "1800000060000000000"`.
- [ ] The writer converts each `LocalTs` one time with `to_peer()`, then multiplies the µs of that
  `VenueTs` by 1,000 at the wire edge.
- [ ] Internal fields stay a `LocalTs` or a `VenueTs` with the `_ts_us` suffix. µs resolution stays:
  the last three digits of each OTLP timestamp are 0.
- [ ] Scope: the OTLP wire ONLY. Every other API, record and file keeps the rules above.

## 6. Errors & Failure

- [ ] Two kinds of failure. Never confuse them.
  - **External, expected** (disconnect, malformed frame, HTTP fail, full queue, full disk): typed
    `thiserror` per concept. Variant = what failed + values seen. Handle by policy: reconnect,
    backoff, resync, drop + count.
  - **Internal invariant violation** (bug: corrupt book, impossible index, broken sequence logic):
    assert + panic immediately. Fail fast, fail loud.
  - Replaced 2026-11-13: "Two failure classes." WHY: "failure class" now names the reaction to an
    error (2026-11-13 section on failure classes, below).
- [ ] Queue-full policy = asymmetric by design.
  - IF INPUT queue is full, THEN fatal drain (engine cannot keep up). Exception: untrusted remote
    producer, amended 2026-07-25.
  - IF OUTPUT queue is full, THEN drop + count. Execution command ring: amended 2026-07-27.
- [ ] IF invariant panic occurs, THEN do coordinated bounded drain: flush persistence queue, close
  Parquet clean, exit non-zero.
  - Amended 2026-11-18: a table of the tape that a write fault stopped is not closed. Its open file
    keeps the name `.parquet.part`, with no footer, and the next start removes it. Each other table
    is sealed. Section on tape faults, end of §6. WHY: a table that failed a write cannot write a
    footer that a reader can trust.
- [ ] Drain deadline = HARD. Corrupt state never continues to produce research data.
- [ ] lib = `thiserror`. bin = `anyhow` + `.context()` on every `?`.
- [ ] Forbidden:
  - `Box<dyn Error>` in any API
  - `Result<T, String>`
  - catch-all `Other(String)`
  - `.unwrap()` outside tests (`.expect("WHY")` only at init boundaries)
  - `let _ =` swallow
- [ ] Messages: lowercase, no trailing period, concrete values included, actionable when fix is
  known.
- [ ] Every `pub` `Result` function documents `# Errors`.

### Amended 2026-07-25: INPUT queue fed by untrusted producer

WHY: fatal-on-full says "the engine cannot keep up". A remote peer that floods a UDP port is an
UNTRUSTED producer. IF it can trip `FatalSignal`, THEN anyone who can reach the port has a
remote-kill primitive.

- [ ] IF untrusted remote producer (the link) feeds an INPUT queue, THEN drop + count, not fatal
  drain.
- [ ] Engine-fed input queues keep fatal-on-full.
- [ ] Drop is sound ONLY under one semantic contract. That contract makes a dropped frame
  equivalent to one never sent. Contract: **link topics carry STATE (absolute values), never
  deltas or events.**
- [ ] That contract = constitutional law, not a code comment. WHY: a strategy that folds a delta
  stream through `on_link` breaks silently on first drop.
- [ ] Drop happens at edge. Recording happens downstream of drop, on hot side. Thus tape holds
  only consumed frames (§2).
- [ ] Amended 2026-11-18: each drop is counted for its sender, and a sender above its rate is
  refused. Section on the count by sender, end of §6. WHY: one total count told no operator which
  peer filled the queue, and one flooding peer took the whole queue.

### Amended 2026-07-27: execution command ring

WHY: execution command ring carries EVENTS, not state. Thus the precondition of drop + count fails.

- [ ] OUTPUT-queue drop + count is sound because consumer needs only LATEST state. A dropped frame
  must be equivalent to one never sent.
- An order command breaks that. A dropped `Cancel` leaves an order resting at a price that the
  strategy abandoned.
- [ ] The precondition is RESTORED, not waived.
- [ ] Hot thread never pushes to execution ring directly.
- [ ] Hot thread transitions order state at BANK time into a fixed-capacity buffer.
- [ ] A separate step drains that buffer into the ring, FIFO. It stops on first failure and
  retries next spin.
- This is the latched pattern that `PersistSink::request_seal` already uses for the one record
  that must never be dropped.
- [ ] Thus ring pressure delays the WIRE, never the STATE.
- [ ] Two consequences bind:
  - The command lane is RETRY. A command that the ring refuses stays in the bank, in order, with
    the fold time of its banking (`StampedExecCommand.issued_ts_us`), and goes out at the next
    spin. The sink drops no command, so it has no count of drops.
    - Replaced 2026-11-13: "Sink still counts drops. §6's letter holds, and in steady state the
      count is unreachable." WHY: the sink drops no command, and no code held such a count.
  - Reachable failure becomes the bank's own high-water mark: a full bank is a quoting halt. It is
    one of the two ring-pressure halts (2026-11-13 section on failure classes, below). The bank of
    each venue is full when it holds `ExecCapacity::command_bank()` commands, never at the capacity
    of its `Vec`.
    - Amended 2026-11-13: the bank takes its capacity from the registry rows (§3, 2026-11-13
      section on capacity). It is still full at that capacity.
    - Replaced 2026-11-13: "The bank is full when it holds `COMMAND_BANK_CAPACITY` commands". WHY:
      the constant became a size of `ExecCapacity`, and each venue has its own bank (§8,
      2026-11-13).
    - Replaced 2026-11-13: "It is a message-driven count, not a ring occupancy. Thus §2 replay
      determinism survives." WHY: a bank fills only while the ring refuses its commands, so its
      halt follows ring space.
- IF hot pass gates on ring space instead, THEN hot state becomes a function of how fast the
  ASYNC consumer drained. That is a function of wall time. Replay diverges.

### Amended 2026-08-08: rate-limit pacing

WHY: a venue's order-rate budget gained its first enforcement. An edge-side pacer that silently
delayed placements would falsify the strategy's time-to-market assumption. It would also make
order flow a function of venue budget drain.

- [ ] Rate-limit pacing, like ring pressure, delays the WIRE, never the STATE.
- [ ] Compliant shape = hot-side REFUSAL that is a pure function of message stamps. Placement is
  not minted this spin. It is counted under its own reject reason. Thus a replay refuses at
  exactly the same point.
- [ ] An adapter never withholds a banked command to satisfy a budget.

### Amended 2026-10-14: sim block is inert outside sim mode

WHY: an operator or agent flips mode with a one-line edit, and a mode flip that makes a config fail
at boot is a trap.

- [ ] A `sim:` block can sit under `mode: off` or `mode: live`.
- [ ] It is validated whenever present, in every mode. Thus a flip back to `sim` never surfaces a
  bad value that sat unchecked.
- [ ] It is USED only under `sim`. Code may read it in any mode, but only a simulated venue acts
  on it.
- [ ] `mode: sim` with no block stays refused.

### Amended 2026-11-12: panics unwind in each profile

WHY: the bounded drain after an invariant panic and the last exposure write in `Drop` need unwind,
and `panic = "abort"` skips both.

- [ ] Each profile of the workspace unwinds on a panic: `dev`, `release`, `test`, `bench`, and
  `deploy` of the root manifest. No manifest sets `panic = "abort"`.
- [ ] The panic hook trips the fatal latch, and the drain runs inside its deadline
  (`install_panic_hook`, `engine/src/runtime/assembly/drain.rs`). `ExposureSink` writes its last
  snapshot in `Drop` while the panic unwinds (`engine/src/hot/sink/replacing.rs`).
- [ ] A latency measurement times the `deploy` profile as it ships, with unwind. The build rule of
  `rex-code-latency` that measures with `panic = "abort"` yields to this clause. WHY: determinism
  and the safety of funds rank above low latency (the ranking of §8).
- [ ] IF a change wants `abort`, THEN it amends this clause first, with a drain that needs no
  unwind.

### Amended 2026-11-13: failure classes, two kinds of halt, and the end of the process

WHY: one error gets one reaction, decided once in code. "Halt" named two different stops. The lib
ended the process itself, so no test could drive the drain deadline.

Failure classes:

- [ ] One enum `FailureClass` for the crate, in `engine/src/msg/`: HALT, RESET, RETRY, REJECT. A
  HALT names its kind: quoting, engine, or start refused.
- [ ] Each error enum that reaches a supervisor has `failure_class()`. Its `match` has no catch-all
  arm, so a new variant does not compile until it has a class.
  - Replaced 2026-11-13: "Amended 2026-11-13, as built: only `StartError`, `ConfigError`, and
    `FatalReason` have `failure_class()`. Four error enums reach the drain of the start code with
    none: `PersistError`, `ExposureError`, `SweepFailure`, and `DrainFailure`. The next part of the
    plan, part 3 (money), closes the gap. Until then this clause is the rule, not yet a fact of the
    code." WHY: part 3 gave `failure_class()` to `ExecSessionError`, `PersistError`,
    `ExposureError`, `SweepFailure`, and `DrainFailure`. The clause is a fact of the code.
- [ ] An error that wraps another error gives the class of the inner error, or a higher class.
  Never a lower one.
- [ ] One supervisor acts on the class at each thread root: the driver of the hot thread, the
  session chassis of a venue edge, the root of each writer thread, and the start code. Lower code
  returns the error.
  - Amended 2026-11-13, as built: four roots read `failure_class()`.
    - The session chassis of a venue edge: `halt_on_session_error` (`engine/src/venues/exec/`).
    - The root of the exposure writer: a failed write is RETRY, up to
      `exposure.max_failed_writes` in a row.
    - The drain: `Exit::decide` (`engine/src/runtime/assembly/exit.rs`).
    - The start code: `Exit::refused(class)`, for each refusal
      (`engine/src/runtime/assembly/run.rs`).
  - Amended 2026-11-14, as built: with feature `otlp`, a fifth root reads `failure_class()`: the
    root of the OTLP exporter thread. `Poster::reject` (`engine/src/runtime/otlp/send.rs`) reads
    the class of each `SendFailure`. Each failure is REJECT, so the exporter drops its records and
    counts them. WHY: the export adds one thread, and the root of that thread is the one supervisor
    of its sends.
  - Amended 2026-11-16, as built: two more roots read `failure_class()`. Thus six roots read it,
    and seven with feature `otlp`.
    - The root of both market-data feeds: `run_lane` (`engine/src/venues/edge/lane.rs`). A session
      of a feed ends with a `MarketDataError`, and `run_lane` calls `halt_on_market_data_error`
      (`engine/src/venues/edge/fault.rs`). Each fault is HALT (engine): a decimal that the mantissa
      cannot hold (`FatalReason::MarketDataUnreadable`), and a connection epoch counter that is
      spent (`FatalReason::EpochExhausted`).
    - The root of the simulator edge: `SimActor::run` calls `halt_on_sim_error`
      (`engine/src/venues/sim/error.rs`) for each `SimEdgeError`. A full command lane trips
      `FatalReason::SimulatorCommandLaneFull`, and a full market inbox trips
      `FatalReason::SimulatorInboxFull`. Each is HALT (engine).
    - WHY: three sites of the market-data actors and one site of the simulator actor tripped the
      fatal latch where they met the fault. A full inbox of the simulator panicked.
  - Amended 2026-11-18, as built: one more root reads `failure_class()`, the root of the tape
    writer. Thus seven roots read it, and eight with feature `otlp`. `Faults::table_stopped`
    (`engine/src/tape/fault.rs`) reads the class of each `PersistError` at the root
    `WriterState::run` (`engine/src/tape/drain.rs`). Section on tape faults, end of §6. WHY: the
    first write error ended the writer thread, and nothing told the hot thread.
  - Amended 2026-11-18, as built: one root has no reader. WHY: the fold decides each of its halts
    from messages.
    - The driver of the hot thread: no error enum reaches it. Each halt of the fold is a
      `HaltReason` that the fold decides from messages, and the driver stops at the fatal latch.
    - Two more sites trip by design: an engine-fed input producer at a full ring (the lane table
      below), and the panic hook (a bug has no class).
  - Replaced 2026-11-18: "Amended 2026-11-16, as built: two roots have no reader yet. WHY: part 5
    of the plan owns the tape, and the fold decides each of its halts from messages.", with its
    line "The root of the tape writer: part 5 of the plan." WHY: part 5 built the reader of the tape
    writer.
  - Replaced 2026-11-16: "Amended 2026-11-13, as built: three roots have no reader yet, each with
    its receiving part.", with its line "The market-data actors and the simulator actor: part 4.
    Each of their 4 sites trips the fatal latch at the place of its fault." and its WHY "the plan
    gives the edges of market data and the simulator to part 4, and the tape to part 5." WHY: part 4
    built the reader of the market-data feeds and the reader of the simulator edge.
  - Replaced 2026-11-13: "Amended 2026-11-13, as built: no supervisor reads `failure_class()` yet,
    and only fitness tests read it. Each of the 19 sites that trip the fatal latch trips it itself,
    where the error is found. The next part of the plan, part 3 (money), closes the gap. Until then
    this clause is the rule, not yet a fact of the code." WHY: part 3 built four readers, and the
    13 sites of the execution edges trip through the chassis.
- [ ] An internal invariant violation has no failure class. It is a bug, and it panics (top of §6).

Lanes:

- [ ] Each lane type names its failure class in code, beside the type:

| Lane | When the ring is full | Class |
| --- | --- | --- |
| Engine-fed input queue: market data (`queues.input_capacity`), timer (`queues.timer_capacity`), execution events (`queues.execution_capacity`) | fatal latch, bounded drain | HALT (engine) |
| Market tap ring of the simulator | fatal latch, bounded drain | HALT (engine) |
| Link input queue (`queues.link_capacity`) | drop the frame, count it (2026-07-25) | REJECT |
| Amended 2026-11-17: control queue of the markers (`queues.control_capacity`) | drop the marker, count it. The link actor pushes the pending marker again at the next announce (2026-11-17 section on the marker queue, below) | REJECT |
| State lanes: UI books and events, outbound link, strategy log, latency samples | drop the record, count it | REJECT |
| Exposure lane | keep the newest snapshot | REJECT (of the older snapshot) |
| Command lane | commands stay in the bank, in order, with their bank stamps (2026-07-27) | RETRY |
| Command bank | quoting halt `HaltReason::CommandBankOverflow` | HALT (quoting) |
| Persistence seal | try at the request and at each spin, up to `persistence.seal_try_limit` tries, then quoting halt `HaltReason::SealNotAccepted` | RETRY, then HALT (quoting) |
| Amended 2026-11-18: status queue of the tape writer | fatal latch, bounded drain | HALT (engine) |
| Copy lane of the OTLP export (feature `otlp`): each line that the log drain writes | drop the copy, count it (2026-11-13, the OTLP export) | REJECT |
| Rows lane of the OTLP export (feature `otlp`): each row of the metrics thread | drop the row, count it (2026-11-13, the OTLP export) | REJECT |

- [ ] Amended 2026-11-13: the writer-status input queue is an engine-fed input queue: a full ring
  is HALT (engine). It holds 4 messages, and it exists only when execution is armed. Each writer
  sends at most one `WriterFailed` in a run, so the ring cannot fill in a correct run. WHY: a
  writer failure halts quoting at one message of the input sequence, so a replay halts at the same
  point.
  - Amended 2026-11-18: the tape writer has a writer-status queue of its own. It is an engine-fed
    input queue too, and holds 4 messages (`WRITER_STATUS_CAPACITY`). It exists when the config has
    a `persistence:` block, in each mode, armed or not. Its id is the last id of the registry. The
    tape writer is its one producer and sends at most one `WriterFailed` in a run. The queue of the
    exposure writer still exists only when execution is armed. WHY: the SPSC ring has one
    producer, which the exposure writer owns, and a tape exists with execution off too.
- [ ] Amended 2026-11-18: the link input queue counts each drop for its sender and refuses a sender
  above its rate (section on the count by sender, end of §6). The persistence seal is also
  requested by the fold at each seal border of message time and at each park, and a request while
  a seal waits keeps its count of tries (section on the seal by message time, end of §6).
- [ ] Amended 2026-11-13: the exposure lane keeps its class. Its ring carries whole snapshots from a
  pool made at start, and the reader hands each snapshot back. WHY: a snapshot at the size of the
  registry cannot cross a ring by value, and the hot thread allocates nothing.
- [ ] Amended 2026-11-16: the market inbox of the simulated venue is no lane, so it has no row. It
  is a capacity event. At a full inbox the venue keeps each event that it holds, evicts nothing, and
  returns `SimEdgeError::InboxFull`. The root of the simulator edge then trips
  `FatalReason::SimulatorInboxFull`: HALT (engine). WHY: an evicted event would change which orders
  filled, so the run would not be valid.

Two kinds of halt:

- [ ] Quoting halt (`HaltReason`): quoting stops, and the engine sweeps its own orders. State stays,
  and the process stays up. Only a person starts quoting again.
- [ ] Engine halt (`FatalReason`): the fatal latch trips with a typed reason, and the first reason
  stays. The drain runs inside its deadline, and the exit code is not 0.
- [ ] Amended 2026-11-16: the quoting halt of an unproven order, `HaltReason::UnprovenOrderResting`
  (label "an order the engine cannot prove its own rests on its market", link code 15, appended).
  The fold halts at one message of the kind `ExecKind::UnprovenOrder`, before it reads the
  provenance of the event, and changes nothing else: no slot, no ledger move, no callback. Only
  the Polymarket edge sends that kind, at the first read of a token (§8, the execution contract
  of part 4). WHY: the fold could book none of the fills of that order, and the third Never line
  forbids its cancel, so a person decides. A replay halts at the same message.
- [ ] A refused start is a HALT. The start returns a typed error before an edge can send an order,
  and the exit code is not 0. Only a person starts the engine again.

Two ring-pressure halts:

- [ ] Two halts follow ring space, not messages: the bank halt (`CommandBankOverflow`) and the seal
  halt (`SealNotAccepted`). Each is a designed capacity event: a consumer that stalls for a whole
  bank, or for a whole cap of tries.
- [ ] A replay of a tape gives the same state up to the first ring-pressure halt. A comparison of
  two runs ends at the message where one of them shows such a halt.
- [ ] The state digest leaves out each field that reads ring space: the commands that the bank
  holds, a pending seal and its count of tries, and the drop counters.
  - Amended 2026-11-18: the count of seal borders that the fold passed is state, and the digest
    holds it. Section on the seal by message time, end of §6. WHY: the count follows the spins,
    not ring space.
- [ ] No other state follows ring space (2026-07-27).

The end of the process:

- [ ] The lib never ends the process: no `std::process::exit` below `engine/src/`, outside
  `engine/src/bin/`.
- [ ] The lib returns `Exit` to the binary, and the binary sets the exit code. Only a graceful exit
  gives code 0.
- [ ] IF the drain passes `engine.drain_deadline_ms`, THEN the watchdog calls the forced-exit
  function that the binary gives, one time.

### Amended 2026-11-13: fatal reasons and the exit

WHY: a fatal reason was a string behind a lock, made at 20 sites, and the exit was a flag and a
string. A sweep that gave up with orders at rest reported a clean sweep.

- [ ] `FatalReason` names each trip of the fatal latch: one variant for each cause, with only plain
  copied values. Its `failure_class()` is HALT (engine). Text that no value can hold, such as a
  parse error, goes to the ERROR line that the site writes before it trips.
- [ ] `FatalSignal` keeps the first reason, with no lock. The reason is set before the flag, so a
  reader that sees the flag also finds the reason.
- [ ] The drain decides the exit one time, from what it saw: `Exit::decide(DrainOutcome)`. A fatal
  reason names the exit first, then a passed deadline, then the first failed step of the drain.
  Only a request to stop with none of the three gives `Exit::Graceful`.
- [ ] `Exit` is `Graceful`, `DrainFailed`, `DeadlinePassed`, `Halted(FatalReason)`, or
  `StartRefused`. `Exit::code` is 0 only for `Graceful`.
- [ ] `run_trading_engine::<S>(strategy_id, te_id, forced_exit)` returns the `Exit`. The binary
  gives the forced-exit function and exits with `Exit::code`.
- [ ] IF an exit sweep passes its deadline with orders at rest, THEN the session reports
  `SessionOutcome::SweepPastDeadline` with the client ids of those orders, never `Swept`. The
  drain counts it as a failed step, so the exit is `DrainFailed`.

### Amended 2026-11-13: money never ends in a log line

WHY: a log line is not state. A full lane drops it (§7), a replay does not fold it, and no reader
sums it. Thus the ledger loses money that only a log line holds. The ledger and the venue then
differ, and no record says why.

- [ ] Each fill and each fee of an own order that reaches the fold has at least one of three
  outcomes:
  - the ledger moves
  - quoting halts
  - the fold sends an incident record and increases the count of its kind
- [ ] A hot log line can go beside the outcome, never in its place.
- [ ] The fold never books money by a guess. IF it cannot book a fill with certainty, THEN quoting
  halts, and the ledger stays as it is:
  - An event of an own order with a fill and no slot: `HaltReason::OrphanFill`, with an incident
    record. An event with no fill moves no money: a count only.
  - A prior-run order with no free slot: `HaltReason::PriorRunOverflow`.
  - Amended 2026-11-14: the first report of a prior-run order seats it at the total of that report,
    and the fold books none of that total. IF the total is above 0, THEN quoting halts
    (`HaltReason::PriorRunFillUnbooked`), with an `OrphanFill` record of the order and its total.
    Later steps book as usual. WHY: the restored position can already hold that total, so a booking
    would count it two times.
  - Amended 2026-11-16: a read can name an own order of this run with no slot. The fold seats the
    order at the totals of the read, with its fee memory at that total, and books none of that
    total. IF the total is above 0, THEN quoting halts (`HaltReason::OrphanFill`), with one
    `OrphanFill` record that holds the total. A read with nothing filled seats the order with no
    halt. Pin: `an_adopted_order_books_none_of_its_cumulative`. WHY: the ledger can already hold
    part of that total from the slot that went, so a booking would count it two times.
- [ ] The fold books a fee one time for each trade id. It never books a fee two times.
  - Amended 2026-11-14: a report of a prior-run order that does not raise its total, at or below
    the total of its seat, books no fee and leaves an `UnbookedFee` record. WHY: the earlier run can
    have booked that fee.
- [ ] A fee in an asset other than the quote asset goes into the fee row of that asset. A fee that
  the fold does not book leaves an incident record.
- [ ] A position at the close of its market window leaves an incident record. Its basis counts as a
  loss in the loss budget of its quote asset, until a settlement message closes it.
- [ ] An incident record names its kind: a fee gap, a stranded position, an orphan fill, or an
  unbooked fee. The count of each kind is hot state, a function of the messages (§2).
- [ ] The tape writer writes the incident table whenever the tape exists, as it writes the table
  of rotations. No config can remove it. WHY: an incident is a money event.
- [ ] A run with no tape has no incident table. Its counts stay in hot state, beside the hot log
  lines. IF the persistence ring is full, THEN it drops the record and counts the drop. The
  incident count stays.

### Amended 2026-11-13: the OTLP export

WHY: a collector is an outside service. Its faults must never reach quoting or the engine, and the
Never line on a timeout binds each request of the export.

- [ ] Two lanes feed the exporter, the copy lane and the rows lane. Each one has the class of a
  state lane (table of the 2026-11-13 section on failure classes), named beside its type.
- [ ] A send of the export is one HTTP request. IF it fails by any cause (a status that is not 2xx,
  a redirect included, a collector that the exporter cannot reach, no answer within 500 ms), THEN
  the exporter drops its records, counts them, and never sends that request again. The next request
  carries only newer records. Class: REJECT.
- [ ] The HTTP client of the export keeps no idle connection, retries nothing, and follows no
  redirect, so no library sends a request of the export again or sends it to another host.
- [ ] A pass sends its logs request and its metrics request at the same time, so a pass ends within
  500 ms.
- [ ] A fault of the collector never halts quoting or the engine. The exporter holds no fatal latch,
  no input queue and no order lane.
- [ ] A refused `otlp:` block is a HALT (start refused), in preflight, before the lease.
- [ ] The exporter stops in the drain after the metrics thread and before the log, in at most
  1,000 ms: the pass in flight, then one last pass. Its failures are no `DrainFailure`, so a run
  whose collector is down exits graceful.

### Amended 2026-11-17: a marker has its own queue and its own writer handle

WHY: a marker rode the link input queue behind the payloads of peers. Under a payload flood its
push failed, so the edges parked at once and the fold did not. The type of the run-state cell let
each holder write it, and only a comment stopped an edge.

The marker queue:

- [ ] The link actor pushes each marker to an input queue of its own, the control queue. It never
  pushes a marker to the link input queue.
- [ ] The control queue exists only when the config has a `link:` block. It counts toward the cap
  of 20 input queues (§2).
- [ ] Its id comes after the ids of the market-data groups, the timer, the link input queue, each
  execution queue, and the writer-status queue. WHY: each recorded message holds its queue id, so a
  new queue takes the next id and shifts no other id.
- [ ] Its capacity is `queues.control_capacity`, default 16. The config refuses 0, as for each
  other queue key.
- [ ] IF the control queue is full, THEN the link actor drops the marker and counts the drop:
  REJECT (lane table of the 2026-11-13 section on failure classes). The exception of 2026-07-25 for
  an untrusted remote producer covers it: a remote controller decides each marker. A marker
  carries a whole run state with its epoch, never a delta, so a dropped marker is equal to one never
  sent.
  - Amended 2026-11-18, as built: the producer counts the drops (`ControlQueueProducer::dropped`)
    and writes one WARN line at each power of 2 of the count. `LinkCounts` holds no count of them.
    WHY: the producer is the one place that sees the full ring, and the push runs in the task of
    the link actor.
- [ ] Level repair: the link actor pushes the pending marker after each claim of control, after a
  dead-man park, and again at each announce (1 s). A marker is pending while the acknowledged state
  differs from the desired state. A second copy changes nothing, because the fold applies only a
  higher epoch.
- [ ] The repair has no cap and no halt of its own. A hot thread that pops no marker also pops no
  spin tick, and the full timer queue halts the engine (HALT (engine), lane table).
- [ ] The fold takes a marker in the one order of ingress (`select_oldest`): the oldest received
  stamp first, and a tie to the lower queue id. Thus a marker waits only for the messages that are
  not newer than it, and a payload flood never delays it past its next pop.
- [ ] The write end `ControlQueueProducer` names its class beside its type, as each lane type does.
  It sits beside the other write ends of the input queues, in the concept module
  `engine::hot::ingress`. The write ends `QueueProducer`, `LinkQueueProducer`, and
  `touched_input_ring` move there too, and `engine::hot::spawn` re-exports them, so their paths in
  §8 hold.
  - Amended 2026-11-18, as built: `SimTapGate` moved there too, and `LinkPush`, the outcome of a
    push to the link input queue, is new there. `engine::hot::spawn` re-exports all six:
    `ControlQueueProducer`, `LinkPush`, `LinkQueueProducer`, `QueueProducer`, `SimTapGate`, and
    `touched_input_ring`. `ControlQueueProducer::push(desired, now)` builds the marker itself, and
    `ControlPlane::push_pending` is the one caller in the link actor.
- [ ] Pins: `payload_flood_never_delays_a_marker_past_its_next_pop`, and the control queue as a
  case of `each_lane_full_gives_its_class`.

The writer handles:

- [ ] The run state has three handles, and one constructor gives all three:
  - `DesiredWriter`: only the link actor holds it, inside `ControlPlane`. It writes and reads the
    desired state, reads the acknowledged state, and gives `pending()`.
  - `DesiredReader`: each edge holds one. It reads the state and nothing more. It is `Clone`.
  - `AcknowledgedWriter`: only the hot thread holds it. The fold stores the state that it applied.
    It is not `Clone`.
- [ ] No edge can write the run state. An edge that calls a write does not compile.
  `DesiredReader` has no conversion into a writer, and neither writer is `Clone`. Pin:
  `edge_handle_cannot_write_run_state`.
- [ ] The handles take the place of `RunStateCell` and `RunControlGate`
  (`engine::runtime::shutdown`) in each holder, and each keeps the visibility of the field that it
  fills. The `pub` contexts of the Binance and Polymarket adapters hold a `DesiredReader`, and the
  setup of the link actor holds the `DesiredWriter` (§8, 2026-11-17).
- [ ] Amended 2026-11-18, as built, in `engine::runtime::shutdown`:
  - The one constructor is `RunStateHandles::new()`, and `Default` calls it. Its three `pub`
    fields are the handles: `desired_writer`, `desired_reader`, and `acknowledged_writer`.
  - `DesiredWriter` gives `desired`, `acknowledged`, and `pending`. Its write, `accept_if_newer`,
    is `pub(crate)`.
  - `DesiredReader` gives `state` only. `AcknowledgedWriter` has no read, and its `store` is
    `pub(crate)`.
  - `RunControlGate` is gone. `RunStateCell` stays as a private type inside each handle.
  - An engine with no `link:` block drops its `AcknowledgedWriter` at start, so nothing reads it.
  - WHY: the first pass named the handles before they were built.

### Amended 2026-11-17: only a listed controller asserts a run state

WHY: the link took a run assertion from each sender with the token, and the highest epoch wins.
So one forged frame locked out each real controller until a restart. Each claim refreshed the timer of
the controller loss before the control plane judged it. An epoch of 2^63 or more read back as
another state at another epoch.

The list of controllers:

- [ ] The config lists the controllers: `link.controllers`, a list of IP addresses. The control
  plane holds the list.
- [ ] An entry holds no port. The relay sends each tab from a socket of its own, with a port that
  the OS selects, so no port names a controller. The control plane compares only the IP address of
  the sender of an assertion with the list.
- [ ] IF the address is not listed, THEN the control plane refuses the assertion and counts it for
  that address in `LinkCounts`: REJECT. The assertion changes neither the desired state nor the
  pending marker, and it does not refresh the timer of the controller loss. The subscription of the
  frame to its topics still holds.
  - Amended 2026-11-18, as built: the verdict is `AssertionVerdict::UnlistedSender`. The control
    plane counts by IP address in `RefusedAssertions`: 16 rows, and each address past the 16th
    adds to one shared count, `past_table`. The link actor copies the table into
    `LinkCounts.refused_assertions` at each announce. The control plane writes one WARN line for
    an address at each power of 2 of its count. WHY: a table made at start holds a fixed count of
    rows, and an address counts as one sender whatever its port.
- [ ] A subscribe at epoch 0 claims no control (`AssertionVerdict::NoClaim`). The control plane
  neither refuses it nor counts it.
- [ ] Only an assertion of a listed controller refreshes the timer of the controller loss. Thus the
  dead-man rule (`on_controller_loss: idle`) parks the engine when each listed controller is silent
  for `ControlPlane::SILENCE_LIMIT`, whatever another address sends.
  - Amended 2026-11-18, as built: the order in `ControlPlane::on_assertion` is epoch 0
    (`NoClaim`), then the address check, then the refresh of the timer, then the check of the top
    epoch, then the highest-epoch store. Thus each claim of a listed address refreshes the timer: a
    stale claim and a claim at the top epoch too. The dead-man rule parks only after the control
    plane took one claim of a listed address. `SILENCE_LIMIT` is 5 s, the lifetime of a
    subscription. WHY: a listed controller that sends a stale claim is still alive.
- [ ] An engine with an empty list accepts no assertion, and its start says so in one line. The
  list is empty by default. Each shipped config with a `link:` block lists the host of its relay,
  `127.0.0.1`.
  - Amended 2026-11-18, as built: the line is the WARN line "link.controllers is empty — this
    engine accepts no run assertion", which `ControlPlane::new` writes when the task of the link
    actor starts. The config takes each entry as an `IpAddr`, so an entry with a port does not
    load.
- [ ] The limit of this form: each process on a listed host that has the token can assert. The list
  keeps each other host out. The message code of the link (stage 3 of the master plan) names the
  sender exactly.

The epoch:

- [ ] The controller epoch is a newtype, the type of `RunAssertion.epoch`. Its one wire form
  refuses 2^63 and above at decode, with the value seen, so the subscribe and the lifecycle frame
  refuse alike. The refusal is counted, and the desired state does not change: REJECT. WHY: the
  run-state cell keeps the state in the top bit of the word of the epoch.
- [ ] The control plane also refuses an assertion at epoch 2^63 - 1 and counts it with the refused
  epochs. Decode and the cell still round-trip that epoch. WHY: the dead-man park takes the desired
  epoch plus 1, so it must always have an epoch.
- [ ] Amended 2026-11-18, as built: the two refusals have two counts.
  - The newtype is `ControllerEpoch` (`engine::msg::run_state`), with `MAX` = 2^63 - 1. It is the
    type of `RunAssertion.epoch`, `Subscribe.desired_epoch`, and `Lifecycle.acknowledged_epoch`.
  - The decode refusal is `LinkDecodeError::EpochOutOfRange { found }`. It refuses the whole frame,
    so the topics of that subscribe are not refreshed either. `LinkCounts.decode_failed` counts it,
    with each other decode refusal, and it has no count of its own.
  - The control plane refuses `ControllerEpoch::MAX` from a listed address with
    `AssertionVerdict::EpochTooHigh`, and counts it in `refused_epochs`, which the link actor
    copies into `LinkCounts.refused_epochs`. From an address that is not listed, the same epoch is
    `UnlistedSender`.
  - WHY: the decode refuses before the control plane sees the assertion, so the inbound path
    counts it. The first pass named one count for both.
- [ ] Pins: `each_u64_epoch_is_refused_or_round_trips`,
  `assertion_from_unlisted_address_changes_nothing`, and
  `start_then_stop_from_a_tab_parks_the_fold` (from a tab through the real relay to the fold, and
  back).
  - Amended 2026-11-18, as built: L4 and L5 sit in `engine/tests/fitness/link/controller.rs`. L16
    sits in `engine/tests/fitness/operator/tab_to_fold.rs`, under feature `web`, so only the run
    with all features runs it. WHY: L16 starts the real relay, which builds only with feature
    `web`.

### Amended 2026-11-17: a tape file has its final name only when it is complete

WHY: a tape file had its final name from its create, before it had a footer, and nothing synced
it. A reader that listed the files during a run, or after a crash, opened a file with no footer.

- [ ] The writer of the tape writes each file under `<name>.parquet.part`. The file takes its final
  name `<name>.parquet` only when it is complete, in this order: the last rows, the footer, a sync
  of the file, the rename, and a sync of its folder.
- [ ] The final name keeps its form (§7, 2026-07-29):
  `{persistence.dir}/{strategy-id}/{te-id}[/sim]/{table}/date=YYYY-MM-DD/{HH}-{boot_ts_us}-{seq:06}.parquet`.
- [ ] A reader lists `.parquet` names only. Thus no reader opens a file that is not complete. The
  readers in the repo obey: the retention walk of the writer, the read-back helper of the fitness
  suite, the test of rotations of target `integration`, and `strategies/etl.py`. A new reader
  obeys too.
- [ ] IF the sync of the file or the rename fails, THEN the writer removes the `.part` and returns
  `PersistError::Io` with its path. IF only the sync of the folder fails, THEN the file keeps its
  final name, and the error names the folder.
  - Amended 2026-11-18, as built: the error also names its table, `PersistError::Io { table,
    path, source }`.
- [ ] Amended 2026-11-18, as built: the root of the tape writer reads `failure_class()`. A fault
  stops only its own table, and the writer goes on with each other table. Section on tape faults,
  end of §6.
  - Replaced 2026-11-18: "The root of the tape writer has no reader of `failure_class()` yet
    (2026-11-13 section on failure classes). Until part 5 of the plan gives it one, the first error
    ends the writer thread." WHY: part 5 built the reader.
- [ ] A process that ends with a file open leaves that file as `.part`, with no footer. No writer
  can finish it.
- [ ] The retention walk runs at the start of the writer and after each periodic seal. It removes
  each `.part` whose boot stamp is not the stamp of this writer, at any age. IF it removes one or
  more, THEN it writes one INFO line with the count. It never removes a `.part` of its own boot, so
  it is safe while a file is open.
  - Amended 2026-11-18, as built: the walk runs at the start of the writer and after each
    `PersistRecord::SealAll`, which the fold sends at each seal border and at each park. It does
    not run after a seal at the row cap, at an hour cross, or at shutdown. WHY: the writer has no
    interval of its own now (section on the seal by message time, end of §6).
  - Replaced 2026-11-18: "after each periodic seal". WHY: no periodic seal exists.
- [ ] The walk is safe only while one writer writes a run folder. The identity lock that each
  engine takes in each mode (part 5 of the plan, the fleet server) makes that a fact of the code.
  - Amended 2026-11-18, as built: preflight takes the identity lock in each mode, and the drain
    lets it go last (§2, 2026-11-13 section on preflight). Thus one writer writes a run folder.
  - Replaced 2026-11-18: "Until then, two engines of one identity in mode `off` can each remove
    the open file of the other." WHY: the lock is built.
- [ ] Each step of a write goes through `FileSteps` (`engine::runtime::durable_file`): create the
  folder, create the file, write, sync, rename, sync the folder, and remove. The durable files (the
  nonce file, the exposure file) take the same steps under a temporary name beside the file,
  `<path>.<pid>.tmp`. A test ends or fails a write at any one step through a stand-in (§12,
  2026-11-17 ruling on the file steps).
  - Amended 2026-11-18, as built: the config edit of the fleet server takes the same steps through
    `durable_file::write_through`, and gives the temporary file the permission bits of the old
    file before the sync and the rename (§8, 2026-10-14 section on the agent control plane).
- [ ] Pin: `kill_at_each_write_step_leaves_no_file_without_footer`. A kill after each step leaves
  no file with its final name and no footer. A file that the row cap seals reads back during the
  run, with each row in order. The next start removes each unfinished file of the killed run.

### Amended 2026-11-18: a frame proves its layout

WHY: the envelope carried a version number that a person raised by hand. One forgotten raise let
a peer of another layout read the bytes of a frame as other fields, with no error.

- [ ] The envelope carries `LINK_LAYOUT` (`engine::link`, `engine/src/link/frame.rs`), a
  `LinkHash` of 8 bytes, in the place of the version. The envelope is 86 bytes: magic, layout,
  topic, token hash, strategy hash, sender hash, boot stamp, sequence, MAC.
- [ ] The digest is computed at compile time (FNV-1a, `LayoutDigest` in `engine/src/link/wire.rs`).
  It folds the envelope, each row of the topic table `ENGINE_TOPICS` (name, topic id, body width,
  body digest), and the payload of a strategy topic (topic 16).
- [ ] Each wire type gives its width and its digest: `WireField` has `WIDTH` and `DIGEST`, with no
  default, so a type without them does not compile.
  - A struct folds each field name, with its width and its digest.
  - An enum folds its label and each variant with its tag. Thus the reason tables of the link feed
    (`RejectReason`, `RejectClass`, `HaltReason`) are in the digest.
  - An option folds its two tags, and an array its length.
- [ ] The body length of each topic comes from the widths. The topic table has 8 rows: subscribe 0,
  books 1, events 2, catalog instruments 3, catalog features 4, lifecycle 5, latency 6, history 7.
- [ ] A frame with another layout is refused before its body is read:
  `LinkDecodeError::LayoutMismatch { found, expected }`: REJECT. The link actor counts it in
  `LinkCounts.layout_mismatched` and in `decode_failed`. The order of the decode: length,
  envelope, magic, layout, token, strategy, topic, exact length, body.
- [ ] Peers of two layouts refuse each other, by design.
- [ ] The golden file `engine/tests/fitness/link/link_frames.txt` starts with the line
  `layout 0x<digest>`, then one line for each frame kind. The test compares first: an equal digest
  with other bytes is red, in bless mode too (`POLYSIM_BLESS_LINK=1`). Bless mode rewrites the
  file only for a new digest.
- [ ] Pins, in `engine/tests/fitness/link/wire_digest.rs`:
  `changed_frame_bytes_need_a_changed_digest`, `each_frame_kind_round_trips`, and
  `digest_mismatch_is_refused_before_the_body`.

### Amended 2026-11-18: the count by sender and the rate of a sender

WHY: a drop on the link input queue was one total count that nobody read, so no operator knew
which peer filled the queue, and one flooding peer took the whole queue.

- [ ] `LinkQueueProducer::push(from, message)` returns a `LinkPush`. The producer keeps
  `SenderCounts` (`engine/src/link/senders.rs`): one row for each source `SocketAddr` (IP and port),
  `LINK_MAX_SENDERS` (64) rows, and one shared row `others` past them. Each row counts
  `queue_full` and `over_rate`. The engine never gives a row back while it runs.
- [ ] The rate is `link.sender_rate_per_sec`: the frames of one source address in one heartbeat of
  the link actor (1 s). Its default is `queues.link_capacity`, and the config refuses 0.
- [ ] IF a frame passes the rate of its sender, THEN the producer refuses it before the ring
  (`LinkPush::OverRate`) and counts `over_rate`: REJECT. A frame inside the rate counts toward the
  rate, also when the full ring then drops it (`queue_full`, and the total `dropped`).
- [ ] At each announce the link actor starts a new rate period and copies the table into
  `LinkCounts.senders`. It writes one WARN line for a sender at each power of 2 of its refusals.
- [ ] The metrics thread reads `LinkCounts` through `Metrics::watch_link_counts`. At each interval
  close it writes one `SenderDropRow` for each sender with a row, a sender with no loss included,
  and one row for `others` when it is not 0. With feature `otlp` the rows go out as
  `polysim.link.sender_drops`, with the attributes `polysim.sender` and `polysim.cause`.
- [ ] The hot thread reads nothing new. The counts live in the producer, which the task of the link
  actor runs.
- [ ] Pins: `fold_state_equals_refold_of_consumed_link_frames` and
  `link_drops_are_counted_for_each_sender` (`engine/tests/fitness/link/link_input.rs`), and
  `link_record_and_link_callback_have_independent_gates`
  (`engine/tests/fitness/link/link_ingress.rs`).

### Amended 2026-11-18: a late tab gets the recent events

WHY: a tab that opened during a run got only the catalog and the lifecycle, so its charts were
empty.

- [ ] `HistoryRing` (`engine/src/link/actor/history.rs`) holds the recent event frames. The link
  actor makes it at start, with room for `link.history_capacity` frames (default 300,000: five
  minutes at 1,000 frames a second), and never grows it. A full ring overwrites its oldest frame.
- [ ] The history is topic 7, `history`, with the body of an event frame (`LinkBody::History`). The
  link actor sends it only to a subscriber that names topic 7. A subscription to each topic, the
  default of an engine peer, gets none.
- [ ] Each flush sends history first: at most `link.history_per_flush` frames (default 128) for all
  replays together, the first replay first. Then the live frames go, each in the flush of its
  arrival.
- [ ] At most 16 replays run at a time (`LINK_MAX_SUBSCRIBERS`). A 17th subscriber gets no replay,
  and nothing counts it.
- [ ] IF eviction overtakes a replay, THEN the replay goes on at the oldest frame that the ring
  holds, and `LinkCounts.history_skipped` counts the frames that it missed.
- [ ] A failed send of a history frame is counted in `history_failed`: REJECT. The live frames go
  on. `history_sent` counts the frames sent. The line of each 60 s names `history_failed` and
  `history_skipped`.
- [ ] No code of the hot thread names the ring.
- [ ] The workstation reads the topic into its chart model (`ChartModel::apply_history`), fills
  only.
- [ ] The config refuses 0 for each of the two keys.
- [ ] Pins, in `engine/tests/fitness/link/history.rs`:
  `late_subscriber_gets_recent_events_and_live_frames_still_flow` and
  `history_ring_never_grows_and_never_reaches_the_hot_thread`.

### Amended 2026-11-18: the seal by message time

WHY: the writer sealed each 300 s of its own wall clock. A run and its second fold sealed at other
borders, so a border of a tape file was no function of the messages.

- [ ] The fold requests a seal of each open tape file when a spin crosses a multiple of
  `persistence.seal_interval_spins` (N): `SealBorders::is_crossed_by`
  (`engine/src/hot/dispatch/seal.rs`).
  - It seals one time for each multiple that it reaches, never by `seq % N == 0`.
  - A skipped slot seals at the next spin. A jump over several multiples gives one seal.
- [ ] N defaults to the spins of 300 s at `engine.spin_interval_us`: 300 for the shipped Binance
  config, 3,000 for the shipped Polymarket config. The config refuses 0.
- [ ] The check is one step of the fold of a spin, after the retry of the latched writes and before
  the strategy. It runs in each run state.
- [ ] The count of borders that the fold passed is state, in the state digest.
- [ ] The request sends `PersistRecord::SealAll` through the seal sink. A request while a seal
  waits keeps the count of tries of that seal. A park also requests a seal.
- [ ] The writer has no interval and no clock read that decides a seal. It still seals at its row
  cap (10,000 rows), at an hour cross of the row stamps, at each `SealAll`, and at shutdown.
- [ ] The rows of `rotations` come over a channel of their own, not the record ring. Thus their
  borders follow the reads of the writer. Stage 2 of the master plan owns that table.
- [ ] Pins: `run_and_second_run_seal_at_equal_borders` (`engine/tests/fitness/tape/seal_marker.rs`),
  which compares the file names with the boot stamp masked; the border case of
  `seal_retry_reaches_cap_then_halts`; the zero case of
  `each_refusal_starts_no_thread_and_spends_no_nonce`.

### Amended 2026-11-18: a tape fault has the class of its table

WHY: the first write error ended the writer thread, and nothing told the hot thread. The engine
traded on while no order or fill reached the disk (defect 14, the part of a writer fault).

- [ ] Each table names its kind in `TapeTable` (`engine/src/tape/mod.rs`). `TapeTable::fault_class`
  gives its class, with no catch-all arm:
  - audit tables, HALT (quoting): `orders`, `fills`, `incidents`, `link_frames`
  - research tables, REJECT (of that table): `features`, `trades`, `book_events`, `klines`,
    `rotations`
- [ ] `PersistError::{Io, Parquet, Batch}` name their table, and their `failure_class()` is the
  class of the table. `WriterPanicked` and `DrainInterrupted` are HALT (engine).
- [ ] A fault at any step of a file (open, write, seal) stops its table. The open file of that
  table stays `.part`, with no footer. Each later row of the table is lost and counted. Each other
  table goes on and seals. At the end one ERROR line gives the lost rows and the stopped tables.
- [ ] Audit fault:
  - The writer writes one ERROR line. The first audit fault pushes one `WriterFailed` of
    `FileWriter::Tape` to the status queue of the tape writer (lane table, above).
  - The fold halts quoting with `HaltReason::WriterFailed`.
  - The first audit error is the error of the drain, so the exit is `DrainFailed`.
  - A later audit fault writes its line and counts, and sends nothing.
- [ ] Research fault: one ERROR line and one more stopped table in the count
  (`PersistHandle::stopped_tables`). Nothing halts, and the drain ends `Ok`.
- [ ] The count of stopped tables counts each stop, audit or research.
- [ ] The full ring still drops an audit row and counts it. The flag of defect 14 stands at
  `engine/src/hot/sink/latched.rs` and waits for the audit ring of stage 2.
- [ ] `rotations` is research by open question 3 (a) of the plan. The case for audit goes to stage
  2 of the master plan, with the audit ring.
- [ ] Pins: `audit_write_fault_stops_quoting_and_research_fault_stops_one_table`
  (`engine/tests/fitness/tape/writer_fault.rs`), and the case of the tape writer in
  `each_lane_full_gives_its_class`.

## 7. Logging

- [ ] Bespoke minimal subsystem. No logging framework dependency.
- [ ] Exactly three levels: `INFO`, `WARN`, `ERROR`.
- [ ] `ERROR` captures backtrace + call-site file/line by construction. API makes omission
  impossible.
  - Amended 2026-11-12: a hot `ERROR` line (`hot_error!`) carries file and line through its call
    site and no backtrace. Section on typed hot records below.
- [ ] Producers hand records to dedicated logging thread. No thread blocks or formats inline.
  - Amended 2026-11-12: the hot thread formats nothing. Another thread formats its own line into
    its fixed-size record and does no I/O. Section on typed hot records below.
- [ ] Debug builds: terminal. Release builds: efficient file sink.
- [ ] Hot path steady state emits nothing. `WARN`/`ERROR` from hot path = OK: exceptional by
  definition.
  - Amended 2026-11-13: a hot line about a fill or a fee goes beside its outcome, never in its
    place. Section on money in §6 (2026-11-13).

### Amended 2026-07-20: strategy telemetry

WHY: strategy telemetry is research output, not engine noise.

- [ ] Strategy callbacks can emit per-tick telemetry through the banked ctx logging lane, on a
  dedicated ring.
- [ ] ONE sink per TE run. Release: log file. Debug: terminal. File name: amended 2026-07-25 and
  2026-07-29.
- [ ] Sink carries engine-origin and strategy-origin records alike. Record's `[tag]` column
  (thread tag or strategy id) distinguishes origin. Thus one file loses nothing.
- [ ] Dedicated ring = drop isolation, not routing. A telemetry flood fills its own ring. It does
  not displace engine `WARN`/`ERROR`.
- [ ] Engine hot-path code stays bound by steady-state silence.
- [ ] Strategy records stamp event time (§2 purity). Amended 2026-11-12: they are typed hot records
  that `note!` and `alert!` bank, and the drain makes their text (section on typed hot records
  below).
- [ ] IF lane is full, THEN drop + count (§6).
- Replaced 2026-11-12: strategy records formatted into the fixed-size record at bank time, through
  three text macros, one for each level. Strategy-lane ERROR carried file/line, no backtrace (WHY
  then: capture allocates, and §3 wins on hot thread). A strategy now has no ERROR line.
- Replaced 2026-07-25: sink was ONE per run, named by strategy id. Release file was
  `logs/<strategy-id>-<date>.log`.

### Amended 2026-07-23: `log` facade

WHY: eframe pulls the `log` facade deeper into the graph. The no-framework rule needs an explicit
carve-out.

- [ ] `log` = transitive-only (via reqwest/tungstenite in a native build, via eframe/egui in the
  browser build), never a polysim choice. Amended 2026-10-26: winit and wgpu left the graph with the
  native window.
- [ ] A polysim module must not call `log`.
- [ ] Bespoke subsystem stays the ONLY polysim logging API.
- [ ] No `log` logger is installed. Thus dependency facade emissions route nowhere: they drop at
  the uninitialized facade.
- [ ] Rule intact: polysim does not depend on `log` directly, and does not log through it.

### Amended 2026-07-25: sink name per TE

WHY: N trading engines per strategy fight over one strategy-id-named file.

- [ ] Sink name = two-part identity (§8): `logs/<strategy-id>-<te-id>-<date>.log`. ONE per TE run.
  - Amended 2026-11-12: one file for each UTC day of record time, so a run of several days writes
    one file for each day. Section on typed hot records below.
- [ ] Everything else in the 2026-07-20 amendment stands.

### Amended 2026-07-29: sim runs write separate artifacts

M13 `exchange-sim`. Ratified O5. WHY: simulated fills that read as live ones are an
execution-safety defect, not an observability preference.

- [ ] A run under `mode: sim` writes `logs/<strategy-id>-<te-id>-sim-<date>.log`.
- [ ] Live stem is unchanged BYTE-FOR-BYTE. Thus no existing path moves, and `off`/`live` runs are
  indistinguishable from today.
- [ ] Physical separation, not an in-line marker, is deliberate. WHY: a `[tag]` column
  distinguishes origin only to a reader who already reads. A glob, a tail, or an operator who
  grabs "the log" must not be able to reach simulated fills by accident.
- [ ] Same ruling binds the sibling artifacts:
  - exposure: `<strategy-id>-<te-id>-sim.json`
  - Parquet: below `<strategy>/<te>/sim/`, with an `execution_mode` footer
  - UI: mode badge (O6)
- [ ] Amended 2026-11-16, as built: one value, `RunArtifacts` (`engine::runtime::artifacts`), gives
  each artifact of a run its name, from the mode and the identity of the run. One stem names the
  log and the OTLP service. The value also names the exposure file, the tape folder with the mode
  of its footer, and the console log of `polysim-mcp`. Each name stays byte for byte. The mode
  badge of the UI still reads `ExecutionMode::badge`. WHY: each writer made its own name from the
  mode, so one writer that missed the mode could put simulated fills under a live name.
- [ ] Amended 2026-11-17: a tape file has the name `<name>.parquet.part` until it is complete, and
  its final name keeps its form byte for byte. Section on tape files, end of §6. WHY: a reader
  must never open a file that has no footer.

### Amended 2026-10-14: the MCP server's stdout is the wire

WHY: `polysim-mcp` speaks JSON-RPC on stdout, so one stray byte there corrupts the agent's
session.

- [ ] `polysim-mcp` never writes to stdout. Its own diagnostics go to stderr.
- [ ] An engine it starts never inherits its stdio. stdin is null. stdout and stderr append to
  `logs/<artifact-stem>-console.log`.
- [ ] A sim run gets the `-sim` stem, under the 2026-07-29 ruling.
- [ ] `tracing` = transitive-only, exactly like `log`. It arrives via reqwest -> hyper-util, and
  now via rmcp. No subscriber is installed. Thus its emissions drop.
- [ ] A polysim module must not call `tracing`.

### Amended 2026-10-26: the browser logs to the console, `polysim-web` to stderr

WHY: a browser tab has no thread to drain a ring, no file to open and no terminal. The log macros
still have to work in code that the engine and the tab share.

- [ ] On wasm32 the three log macros print one line each to the browser console, at the matching
  console level. No lanes, no drain thread, no backtrace. Every line still carries call-site file
  and line.
- [ ] The macros stay the ONLY polysim logging API (2026-07-23). Only the sink differs by target.
- [ ] `polysim-web` installs no bespoke log. Its diagnostics are plain lines on stderr, like
  `polysim-mcp`'s. Steady state emits one line per closed tab, never one per datagram.

### Amended 2026-11-12: typed hot records, lanes and drain

WHY: text made on the hot thread cost formatting and a wall-clock read for each line, and a stop
flag lost queued lines with no count.

Records:

- [ ] The hot thread and a strategy write a `HotRecord`: 64 bytes, alignment 64. It holds its call
  site, the message time of the message under fold, and at most 6 words of argument values. The
  hot thread copies raw words and makes no text.
- [ ] Each other thread writes a `TextRecord`: 256 bytes, alignment 64. That thread makes its text
  into the fixed buffer, and the record takes engine time from the one `EngineClock` of the
  process.
- [ ] The log drain makes the text of each hot record from its template and words. One template
  parser serves the compile-time check of each call site and the drain, so the two cannot differ.
- [ ] A hot record takes message time, never a clock read. Thus two replays of one tape give
  byte-equal hot and strategy records (§2).

Macros:

- [ ] Code below `engine/src/hot/` logs with `hot_info!`, `hot_warn!` and `hot_error!`. A strategy
  logs with `note!` (INFO) and `alert!` (WARN) on its banked context. Each other thread keeps
  `info!`, `warn!`, `error!` and `warn_repeating!`.
- [ ] A hot template takes only the placeholders `{}`, `{:?}`, `{:x}` and `{:.N}`, and each argument
  is a `LogValue`. A wrong argument type, a placeholder count that differs from the argument count,
  or more than 6 words fails the build.
- [ ] A hot line keeps the text that its text macro printed for the same values. A value renders
  its `Display` text under `{}` and its `Debug` text under `{:?}`.
- [ ] A hot macro on a thread with no hot lane, or before the first message, makes its text at once
  into a `TextRecord` on the lane of that thread.
- [ ] A strategy has no ERROR line: a broken invariant of its own is a bug, and a bug panics.
- [ ] A hot `ERROR` carries file and line through its call site and no backtrace. An `error!` of
  another thread captures a backtrace on its thread, and the drain turns it into text.
- [ ] `engine/src/hot/` and `strategies/` hold no text macro and no text formatting outside the
  message of an assertion. The fitness test `hot_path_holds_no_text_macro` holds it, with each
  exception named in the test with its reason.
- [ ] On wasm32 the hot macros make their text at once with the renderer of the drain and print to
  the console (2026-10-26 above).

Lanes:

- [ ] One SPSC lane for each producer: each OS thread, each tagged tokio task, and the hot lane and
  the strategy lane of the hot thread. One drain thread reads them all.
- [ ] The hot thread builds its hot lane after it pins, and touches each slot once with a marker
  record. Thus no first write to a slot faults a page in steady state.
- [ ] A full lane drops the record and counts it. The producer never waits. Each drop counter has
  one writer: a load and a store, no locked instruction.
- [ ] The hot lane holds 4,096 records. Each thread lane and the strategy lane hold
  `logging.ring_capacity` records (default 8,192).

Drain:

- [ ] The drain reads each lane in batches and writes through one buffered writer. Its thread pins
  to `engine.housekeeping_core_id` when that key is set. On Linux a failed pin refuses the start.
- [ ] Loss policy: IF a write fails, THEN the drain counts the lost line and reopens the file. IF
  `logging.reopen_limit` reopens in a row fail (default 3), THEN lines go to stderr, and the drain
  still counts. A fault of the log disk never halts the engine: the log holds no engine state.
- [ ] One log file for each UTC day of record time: the drain opens a new file when the date of a
  record changes.
- [ ] At shutdown the drain runs until each lane has ended and is empty, OR until
  `logging.drain_deadline_ms` passes (default 2,000). The config refuses a value at or above
  `engine.drain_deadline_ms`, so the watchdog of the engine never cuts the log flush.
- [ ] `LogHandle::drain` returns the counts and prints them to stderr: lines written, records
  dropped, lines lost at a write error or at the deadline, I/O errors, reopens, and the lanes that
  had not finished at the deadline.
- [ ] The value form of the drain is `pub` by the ruling of §12 (2026-11-12, section on seams that
  change).
- Replaced 2026-11-12: the hot thread and each strategy formatted each line into a text record of
  about 240 bytes and stamped it with a wall-clock read. The drain stopped one pass after a flag. A
  write error reached only `eprintln!`. The date of the file came from its open. A drop counter was
  a `fetch_add`.

### Amended 2026-11-13: the OTLP export copies log lines

WHY: an operator reads the log of a live engine in a standard collector, and the log file stays the
record.

- [ ] Only a build with feature `otlp` and a config with an `otlp:` block exports.
- [ ] The log drain copies each line that it writes into the copy lane of the exporter: time, level,
  tag, module, file, line and text. The drain never waits for the exporter.
- [ ] A copy holds at most 238 bytes of text, as a text record does. The log file keeps the whole
  line.
- [ ] The export is a copy that can lose lines, and it counts each loss. A fact that must not be
  lost never lives only in a log line.
- [ ] The service name of the export is the artifact stem of the run, so the name of a sim run ends
  in `-sim` (2026-07-29).
- [ ] Rule intact: the bespoke log is the only polysim logging API. The exporter calls neither `log`
  nor `tracing`.
- [ ] The exporter logs through its own thread lane, tag `otlp`. Its lines reach the collector like
  any other line.

## 8. Code Organisation

- [ ] Two lib crates hold all substance: `engine` and `lab` (2026-11-12 section below). Binaries
  only compose: load config, wire, start, orchestrate shutdown.
  - Replaced 2026-11-12: one lib crate (`polysim`) held all substance.
- [ ] Module = concept (`hot/`, `venues/`), not type-per-file.
- [ ] Every module opens with 1–2 line `//!` WHY header.
  - Amended 2026-11-12: priority block. The root file of each lib crate (`engine/src/lib.rs`,
    `lab/src/lib.rs`) holds the priority block after the first sentence of its header: one line
    `Priorities, in order:` and its `Never:` lines, max 8 lines (`rex-code-priorities`). WHY: an
    agent breaks a tie by one ranking. It reads that ranking in code, in the one file that it
    opens first, and nowhere else.
  - [ ] The ranking of the owner, word for word:

    ```
    engine:    Priorities, in order: determinism, self-evident architecture, adaptation (venues, market kinds, engines, regions), low latency.
               Never: give a different state or different orders for the same messages in the same order.
               Never: send a request again after a timeout.
               Never: touch an order that this engine did not place.
    lab:       Priorities, in order: determinism, self-evident architecture, adaptation (tiers, signals, sessions), throughput (sweeps).
               Never: show the research agent a test fold or the holdout slice.
               Never: reach a live engine.
               Never: let a replay cross a tape gap.
    ```

  - [ ] Each root file holds its lines without the label `engine:` or `lab:`. IF a root file does
    not hold its block yet, THEN the lines above are the ranking in force.
  - [ ] CAUTION: Do not change a priority block without an instruction from the owner. A changed
    ranking changes each later design decision, and no test reports it. IF the owner changes a
    block, THEN amend this clause on the same day.
- [ ] `hot/`: no async code, no exchange-specific wire formats. Adapters normalize at edge.
  Generalized internal messages = only currency inward. Review-enforced. Workspace split only if
  seam demonstrably needs compiler help.
- [ ] `pub(crate)` = default. `pub` = promise.

### Amended 2026-07-20: strategies live outside lib

WHY: strategy implementations are customer code (§9).

- [ ] Strategy implementations live outside the lib. Folder layout: amended 2026-07-25.
- [ ] The lib keeps the seam: `Strategy`, `StrategyConfig`, `StrategyCtx`, the Actions bank, and
  the runner. Runner: amended 2026-07-25.
- Replaced 2026-07-25: strategy lived in `strategies/<id>/` with main.rs (composition only) +
  strategy.rs + config.yaml. bin name = folder name = strategy id via `env!("CARGO_BIN_NAME")`.
  Runner in the seam was `run_strategy`. Breaking data-format change = new folder/id.

### Amended 2026-07-23: helper modules beside strategy.rs

WHY: a recorder outgrew one readable file.

- [ ] A strategy folder can carry helper modules beside strategy.rs. Declare them from strategy.rs
  via `#[path]`. Thus both compile roots (the bin and the fitness `#[path]` include) resolve the
  same flat sibling files.
- [ ] strategy.rs stays the single entry.
- [ ] The §1 strategies/ exemption from the 500-line cap covers these siblings too. They are the
  same customer code, split only for the reader.

### Amended 2026-07-25: two-part identity, one folder per TE

WHY: a strategy is now a SET of independently deployable trading engines, each bound 1:1 to one
source. Thus identity is two-part and the folder nests.

- [ ] A strategy folder holds one folder per trading engine: `strategies/<strategy-id>/<te-id>/`.
- [ ] That folder carries main.rs (composition only) + strategy.rs + its `#[path]` siblings +
  config.yaml.
- [ ] Identity = `(strategy-id, te-id)`.
  - Amended 2026-11-13: a TE reads one source or more, and identity stays two-part. The WHY above
    keeps its text. Section on several sources, end of §8.
- [ ] bin name = `<strategy-id>-<te-id>`. Path = `strategies/<strategy-id>/<te-id>/main.rs`.
- [ ] Pass both ids as literals to the lib seam's runner, now
  `run_trading_engine::<S>(strategy_id, te_id, forced_exit)`. It returns the `Exit`, and the
  binary exits with `Exit::code` (§6, 2026-11-13).
  - Replaced 2026-11-13: "`run_trading_engine::<S>(strategy_id, te_id)`", which returned the exit
    code. WHY: the lib never ends the process, so the binary gives the forced-exit function.
- [ ] `env!("CARGO_BIN_NAME")` no longer derives identity.
- [ ] Enforcement is structural: a fitness config-guard parses `engine/Cargo.toml` and each
  `strategies/<strategy-id>/Cargo.toml`, and asserts that name/path pair on every `[[bin]]`. Thus
  global bin-name uniqueness is a test failure, not a hope.
- [ ] Cross-TE sharing has exactly ONE authorized shape: a `#[path]` sibling at strategy level
  (`strategies/<strategy-id>/link_schema.rs`), included from each te strategy.rs.
  - Amended 2026-11-12: `link_schema.rs`. The file and its module have the name `link_schema`, in
    each strategy and in the two `#[path]` modules of the fitness target. WHY: the file holds only
    the link schema that the engines of one strategy must agree on. The name `common` says nothing
    about content, so each shared thing would land in it.
  - Replaced 2026-11-12: the file was `common.rs`, module `common`.
- [ ] Otherwise per-TE duplication is accepted (customer code, §9).
- [ ] Breaking data-format change = new te folder/id.
- [ ] Amended 2026-11-18: each te folder also holds `schema.lock`, written by hand. It holds one
  line `column <name>` for each column of the strategy, in the order of their ids, and one line
  `link_schema_digest 0x<hex>`, the digest of its link schema (`schema_hash_of_fields`). Blank
  lines and `#` lines are skipped. The Polymarket publisher has no column, so its lock holds the
  digest only.
  - The fitness test `schema_lock_of_each_strategy_holds` walks each folder below `strategies/`
    that holds a `strategy.rs`, and compares it with its lock. A rename, a removal, a move of a
    column, and a change of the link schema fail. A column added at the end fails until its line
    is added at the end of the lock by hand. The test never writes a lock. For the recorder it
    also holds that the columns 57 to 76 are the link fields in wire order, and the 32 claims of
    its group accessors.
  - `permuted_declaration_decodes_to_equal_tables` decodes each file by its own footer: two orders
    of declaration give equal tables.
  - WHY: the names were frozen by hand for one strategy, in a test, so a rename in another
    strategy broke the reader of its tape with each gate green. The kind of a column waits for
    stage 2 task T6 of the master plan.

### Amended 2026-08-08: venue execution adapter

WHY: three venues implement the execution seam by convention. A fourth needs law, not archaeology.

- [ ] A venue execution adapter is a COMPOSITION over `venues/exec`. It:
  - drives `ExecCore` through its own codec and transport
  - funnels every exec and account event through its ONE assigned input queue, stamped
    monotonically
  - declares its venue physics in a `VenueCapabilities` record and its lease-nonce namespace
    beside it at construction
  - implements the `EdgeDriver` session chassis, not its own lifecycle loop
- [ ] Never re-implement shared exec machinery per venue: core, mirror, in-flight table, resync
  pass, exit lifecycle, synthesized-event constructors.
- [ ] Obligations that no signature can carry live in `engine/src/venues/exec/README.md` and their
  fitness pins:
  - cumulative totals
  - reconcile-never-resend
  - cancel finality
  - readiness order
  - fatal on an undecodable open order
  - Amended 2026-11-13: the README writes nine obligations, and one generic seam suite pins them.
    Section on the execution contract, end of §8.
  - Amended 2026-11-15: adoption by exact match, at a venue that mints the order ids. Section on
    the execution contract of part 4, end of §8.
- [ ] The README = prose half of the adapter contract. Changes to it are seam changes, not doc
  edits.

### Amended 2026-10-14: agent control plane

WHY: an agent operates the engines through an MCP server, and that server must not become a second
place where engine state lives.

- [ ] The MCP server (`engine/src/operator/fleet/`, bin `polysim-mcp`) is an OPERATOR process, not
  an edge actor.
- [ ] It holds no market state and links no strategy. It reaches an engine only through the OS:
  spawn, signal, exit status, files.
- [ ] Every tool is a thin shell: params -> one `Fleet` call -> reply or refusal. The substance
  stays in the supervisor. Thus a CLI or UI can reuse it.
- [ ] `Fleet` is `pub` on its own merit, as the reusable operator supervisor. Its fitness pins
  follow from that ruling and are not the reason for it.
- [ ] All tools live in one file. A second tool family becomes
  `engine/src/operator/fleet/tools/<family>.rs`, with routers merged by `+`.
- [ ] The server makes no edit it cannot read back. `set_execution_mode` writes only when the new
  file parses as the requested mode.
- [ ] Server exit stops every engine it started: SIGTERM first, SIGKILL only past one shared
  deadline.
- [ ] Amended 2026-11-18, as built: the fleet sees an engine that it did not start. WHY: after a
  restart of the server, a running engine, a live one too, was listed as stopped, and a start made
  a second process of it.
  - The fleet probes the identity lock of each engine (§2, 2026-11-13 section on preflight):
    `IdentityLocks::is_held`, a shared lock that it lets go at once. A held lock lists the engine
    as `running elsewhere`, and a start of it is refused with `FleetError::RunningElsewhere
    { identity, lock }`. A row of a child of this server wins over the probe.
  - An engine that starts while the fleet probes its lock is refused as held, by design.
  - The states of a row: `running`, `stopping`, `running elsewhere`, `exited`, and `stopped`.
- [ ] Amended 2026-11-18, as built: a stop waits outside the lock of the fleet. WHY: a stop held the
  one lock of the fleet until the engine exited, so no session listed the engines during a stop.
  - Under the lock, the stop sends SIGTERM and the slot becomes `stopping`. Outside it,
    `StopBatch::settle` waits. Under the lock again, the outcome is recorded.
  - A stop of a slot that is stopping, and a start of it, give `FleetError::Stopping`.
  - The close of the server (`stop_all` of the tools) takes the same three steps.
  - The start tool still holds the lock through the start grace of 2 s, so a list waits behind a
    start.
- [ ] Amended 2026-11-18, as built: the one edit of the server goes through the durable-file
  writer (`durable_file::write_through`), with the permission bits of the old file.
  - IF a step before the rename fails, THEN the config is not changed, and the temporary file is
    removed: `FleetError::ConfigWrite`.
  - IF the remove of the temporary file fails too, THEN both errors come back, with the path of
    the file left behind: `FleetError::ConfigWriteLeftBehind`.
  - IF only the sync of the folder fails, THEN the file holds the new mode but is not proven
    durable: `FleetError::ConfigNotSynced`. Its message says so.
  - WHY: the edit wrote a staging file and renamed it with no sync, and a failed step left the
    staging file beside the config.
- [ ] Amended 2026-11-18, as built: the HTTP server accepts through the one accept loop with a cap
  (§12, 2026-11-17 ruling on the accept loop). Past the cap it takes no new session, writes one
  line to stderr, and keeps each engine that it started. Its exit signal still stops them, and the
  server then exits 0. WHY: an exit of the server would stop each engine that it started.
- [ ] Amended 2026-10-15: `--http <port>` serves the same tools on 127.0.0.1 only, with no auth:
  on a single-operator host, loopback is the auth. A request that carries an `Origin` header is
  refused (the MCP spec requires Origin validation), so no browser quirk around the CORS preflight
  can reach the tools. WHY: one long-lived server is shared by the workstation and every agent
  reply, so an engine started from chat outlives the reply.

### Amended 2026-10-14: the `polysim-bridge` binary (WITHDRAWN 2026-10-26)

Not in force. Record only.

- WHY then: Jet painted the workstation in a web page. The bridge kept the link and the egui-free
  model in Rust and handed the page JSON lines, so no second model could drift from the one the
  egui window folded.
- Text then: `polysim-bridge` declared no required-features and linked no eframe, so it only
  composed. Its stdout carried protocol JSON lines and nothing else. Usage text and errors went to
  stderr. Diagnostics went through the bespoke log (§7): stderr in debug, file in release. The
  gate stayed the two configurations of §14, and the bridge built in both.
- WHY withdrawn: Jet now hosts an app only by HTTP, and embeds that app's own UI. The bridge had
  no reader left, so the workstation itself runs in the browser (below).

### Amended 2026-10-26: the `polysim-web` binary

WHY: Jet hosts an app only by HTTP, so the workstation runs in a browser tab and needs a loopback
server. That server must not become a second place where link or market state lives.

- [ ] `polysim-web` (`engine/src/operator/relay/`, bin `polysim-web`, `required-features = ["web"]`)
  only composes: parse arguments, call `serve`. The substance stays in the lib.
- [ ] It listens on 127.0.0.1 only. No flag binds another address. `--port 0` lets the OS pick.
- [ ] It relays link datagrams BYTE FOR BYTE: one binary WebSocket message = one datagram, both
  ways. It decodes nothing, holds no market state, and no hot-path type derives serde for it. Thus
  a tab is an ordinary link subscriber. The link's own guard, sequence gate and STATE-not-deltas
  contract (§6, 2026-07-25) apply to it unchanged.
- [ ] The guard runs before every route. A request needs exactly one `Host` header that names
  `127.0.0.1`, `localhost` or `[::1]` on the port actually bound. It carries at most one `Origin`
  header, and a present one must equal `http://<Host>`. `Origin: null` is refused. WHY: the relay
  carries START/STOP run control with no login. `Host` stops DNS rebinding. `Origin` stops another
  site from opening the relay. A page always sends `Origin` when it opens a WebSocket, so a
  request without one cannot be another site opening the relay.
- [ ] Stopping it stops nothing on the engines.
- [ ] Diagnostics go to stderr, like `polysim-mcp` (§7, 2026-10-26).
- [ ] The app descriptor (`/jet-app.json`) advertises the MCP server when `--mcp <url>` is given.
  `polysim-web` only repeats that address. It never starts, stops or calls the server.
- [ ] Amended 2026-11-18, as built: the guard sits in its own file,
  `engine/src/operator/relay/guard.rs` (`guard`, `Refusal`, and `is_loopback_authority`, each
  private to the relay). Its rules above hold unchanged. WHY: the guard is the one barrier in front
  of run control, so it is read alone.
- [ ] Amended 2026-11-18, as built: the hello of a tab carries the token hash of `LinkCredential`
  (`engine/src/link/credential.rs`), the one constructor of the link credential. The engine builds
  its guard and its identity from the same credential. Thus the token hash is derived in one
  place. WHY: the relay and the engine derived it in two places, so a change of one of them made
  the guard of the engine refuse each tab.
- [ ] Amended 2026-11-18, as built: the relay accepts through the one accept loop with a cap, the
  flag `--accept-tries` (§12, 2026-11-17 ruling on the accept loop). Past the cap, `serve` returns
  `WebError::Accept`, and `polysim-web` exits with code 1. WHY: the relay retried each accept
  error with no end.

### Amended 2026-10-26: the workstation's link seam and the web surface are `pub` on merit

WHY: §12 (2026-07-28) forbids `pub` for a test's sake. Each item below has a caller that is not a
test: another target, another bin, or another program.

- [ ] The workstation's link seam is `pub`: `LinkSession`, `LinkTarget`, `Delivery`,
  `LinkSessionError`, `LinkStatus`, `ConnectionState` and `ControlVerdict`. Bytes and time go in.
  Deliveries, status and subscribe bytes come out. The session owns no socket, clock, thread or
  randomness, so the browser shell and any native caller drive it the same way. `CatalogAssembly`
  and `Controller` stay `pub(crate)`.
- [ ] `operator::workstation::browser::{start, start_in_canvas}` are `pub`: the ONE boot path, with
  two callers in two compile roots. `polysim-ui` calls `start`, which calls `start_in_canvas`.
  `dom-fixture` calls `start_in_canvas` itself.
- [ ] `operator::relay::{WebConfig, ArgumentError, serve, WebError}` are `pub`: the surface that the
  `polysim-web` bin composes. `ArgumentError` is `pub` because `WebConfig::parse` returns it.
- [ ] A fitness test that names these follows from this ruling. It is never the argument for it.
- [ ] Amended 2026-11-17: the control plane of the link is `pub` on the same merit as the link seam
  of the workstation: time goes in, and it owns no socket, clock, or thread. Section on the `pub`
  items of part 5, end of §8.

### Amended 2026-10-26: `dom-fixture` is a bin that holds its own scenes

WHY: Trunk builds only `[[bin]]` targets, so the fixture page could not stay an example.

- [ ] `dom-fixture` (`tools/dom-fixture/`) is a `[[bin]]`, a dev page that no deployment builds.
  Its scenes and builders live beside its entry.
- [ ] It is the one exception to "binaries only compose". The scenes are its substance. In the lib
  they would be `pub`, which makes fixture data a promise.
- [ ] It declares no required-features. The fitness config-guard pins that, and pins `polysim-ui`
  the same way: Trunk builds both with no flags.
- [ ] On wasm32 it paints. On native it prints the risk-scene text dump.

### Amended 2026-11-12: two lib crates

WHY: a live engine build must compile no research code, and research must reach the engine only
through its public API. Two crates let the compiler hold that seam, so the split meets the
workspace clause at the top of §8.

- [ ] The repo is one Cargo workspace. Root `Cargo.toml` = workspace manifest. Members: `engine`,
  `lab`, `strategies/strat-micro-recorder`.
- [ ] `engine`: each part that a live engine links. Features `mcp` and `web`. Binaries
  `polysim-mcp`, `polysim-ui`, `polysim-web`, `dom-fixture`.
- [ ] `lab`: scoring and research over recorded history. It depends on `engine`. It holds only its
  root file until research code arrives.
- [ ] `engine` never depends on `lab`. A crate that only `lab` needs never enters
  `engine/Cargo.toml`.
- [ ] Each strategy is one package, `strategies/<strategy-id>/Cargo.toml`, with one `[[bin]]` for
  each TE. It depends on `engine`.
- [ ] A binary that a fitness test starts stays in package `engine`. WHY: Cargo gives a test the
  path of a binary of its own package only.
- [ ] Profile tables (`[profile.deploy]`) live only in the root manifest. WHY: Cargo reads profile
  tables only there.
- [ ] The root of each lib crate holds `#![forbid(unsafe_code)]` (§14). The workspace lint table
  denies `unsafe_code` in each other target. The fitness root holds the one
  `#![allow(unsafe_code)]`, with its WHY: its counting allocator implements `GlobalAlloc`.
- [ ] A third lib crate needs its own amendment, by the workspace clause at the top of §8. A new
  strategy package needs none.

### Amended 2026-11-12: new path names

WHY: the workspace puts the code of polysim into package `engine`, and each top folder of
`engine/src/` names its job. A path in this law that names the old place sends a reader to a file
that is gone.

- [ ] The lib, its tests, and its fixtures sit below `engine/`: `engine/src/`,
  `engine/tests/fitness/`, `engine/tests/integration/`, `engine/fixtures/<venue>/`. Each fixture
  keeps its polysim path below `engine/`.
- [ ] `engine/src/` holds `bin/` and eight top modules, one job each. Column "Was" = folder or file
  below the source root of polysim:

| Folder      | Job                                                                                          | Was                                                                  |
| ----------- | -------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `msg/`      | messages that cross domains, with `ids`, `time`, `labelled_enum`, `run_state`                | `msg/`, `ids.rs`, `time.rs`, `labelled_enum.rs`                      |
| `log/`      | bespoke log (§7)                                                                             | `log/`                                                               |
| `hot/`      | hot thread, with `sink/`                                                                     | `hot/`, `sink/`                                                      |
| `venues/`   | one folder for each venue, `edge/` (session chassis + toolbox), `exec/`, `sim/` (simulator)  | `adapters/`, `adapters/exchange_sim/`                                |
| `link/`     | state frames between engines                                                                 | `link/`                                                              |
| `tape/`     | Parquet tables, off the hot path                                                             | `persist/`                                                           |
| `runtime/`  | `config/`, `registry/`, `exposure/`, `secrets.rs`, `shutdown.rs`, `assembly/` (start + stop) | `config/`, `registry/`, `exposure/`, `secrets.rs`, `shutdown.rs`, `runtime/` |
| `operator/` | `workstation/`, `relay/` (web relay), `fleet/` (MCP server)                                  | `workstation/`, `web/`, `mcp/`                                       |

- [ ] Two files have new names: `operator/fleet/supervisor.rs` (was `mcp/fleet.rs`) and
  `operator/relay/tab.rs` (was `web/relay.rs`). WHY: a module with the name of its folder fails
  the lint `clippy::module_inception`.
- [ ] Amended 2026-11-18: the files that part 5 added below `engine/src/`, each with one job:
  - `link/actor/`: `control_plane.rs`, `inbound.rs`, `outbound.rs`, and `history.rs`; the root
    file `mod.rs` keeps the wiring and the heartbeat.
  - `link/credential.rs` (the one link credential) and `link/senders.rs` (the count by sender).
  - `operator/accept.rs`, the one accept loop of the relay and the fleet server. It sits in
    `operator/`, because `relay/` builds only with feature `web` and `fleet/` only with `mcp`.
  - `operator/relay/guard.rs` and `operator/fleet/stop.rs`.
  - `hot/dispatch/seal.rs` (the seal borders) and `tape/fault.rs` (the faults of the tape writer).
  - The write ends of the input queues moved from `hot/spawn.rs` to `hot/ingress.rs`.
  - WHY: each new box of part 5 has a file of its own, so a reader finds it by its name.
- [ ] Public paths: `polysim::…` = `engine::…`. An exported macro sits at the crate root
  (`engine::info!`). Each other path follows its folder: `engine::venues::sim::…`,
  `engine::tape::…`, `engine::runtime::config::…`.
- [ ] `engine::runtime::{StartError, ExecutionLease, Exit, run_trading_engine}` keep their path:
  the root file of `runtime` exports them from `assembly`, with the start and exit items of
  2026-11-13 (`Preflight`, `StartPlan`, `Engine`, `DrainOutcome`, `Trigger`, `DrainFailure`,
  `DrainDeadline`, `DrainWatchdog`, `ForcedExit`) and the identity lock of 2026-11-18
  (`IdentityLock`, `IdentityLocks`).
  - Amended 2026-11-18: `check_symbol_order_capacity` is crate-private
    (`engine/src/runtime/assembly/preflight.rs`), and no root file exports it. A test reaches the
    check through `Preflight::run_with` with a stand-in venue port. WHY: only a fitness test called
    it, and §12 (2026-07-28) forbids `pub` for a test's sake.
  - Replaced 2026-11-18: "`engine::runtime::{StartError, ExecutionLease, Exit,
    check_symbol_order_capacity, run_trading_engine}` keep their path". WHY: the function is
    crate-private.
  - Replaced 2026-11-13: "`engine::runtime::{EngineError, ExecutionLease, ExitReport,
    check_symbol_order_capacity, decide_exit, run_trading_engine}` keep their path". WHY:
    `EngineError` became `StartError`, `ExitReport` became `Exit`, and the exit decision became
    `Exit::decide`.
- [ ] `engine/tests/fitness/` groups its modules by the top module that they test: `hot/`,
  `venues/`, `link/`, `tape/`, `runtime/`, `operator/`. Folder `scans/` holds the tests that read
  the repo as data.
- [ ] `strategies/`, `tools/`, `browser/`, `wiki/`, `static/` stay at the root. `engine/Cargo.toml`
  names its targets below `tools/` by `../tools/`.
- [ ] Names that stay, because a test or a data file reads them: each binary name, each runtime
  string that holds `polysim` (thread names, lock folder, log stem), each nonce file name, each
  data and log path, and version `0.1.0` of package `engine` (the Parquet footer holds it).
- [ ] This amendment rewrote each path of this file to its new form. An earlier amendment keeps its
  date and its WHY. A record marked Replaced or WITHDRAWN keeps its old text.

### Amended 2026-11-12: depth of venue folders

WHY: each venue has one layout. A reader who knows one venue finds each part of the next venue at
the same path. A flatter tree gives each venue its own shape.

- [ ] Each venue folder `engine/src/venues/<venue>/` uses the same layout. Its execution parts sit
  in `venues/<venue>/exec/<part>/`, for example `actor/` and `codec/`: four folder levels below
  `engine/src/`.
- [ ] The simulator core has the same depth: `venues/sim/core/schedule/`.
- [ ] Four levels is the limit, and it holds only below `engine/src/venues/`. Here this clause wins
  over the folder rule of `rex-code-folder-management` (max three levels below `<package>/src/`).

### Amended 2026-11-13: the fold, start, and exit seams are `pub` on merit

WHY: §12 (2026-07-28) forbids `pub` for a test's sake. Each item below has a caller that is not a
test, or sits in a module that is a concept in its own right, and each clause names it.

- [ ] `HotEngine::state_digest` (`engine::hot::dispatch`) is `pub`: the fold's own statement of its
  state, the subject of the first Never line. `dispatch` never calls it. Merit: the fold is a
  concept in its own right, and the timing harness builds a `HotEngine` outside the crate
  (`tools/latency-probe/rig.rs`).
- [ ] `select_oldest` (`engine::hot::ingress`) is `pub`: the pure rule that picks the next message
  from the queue heads. It is the one place that decides the order across queues. Merit: the
  module is a concept in its own right, and the timing harness builds `IngressQueues`
  (`tools/latency-probe/idle.rs`, `tools/latency-probe/step.rs`).
- [ ] `SpinSchedule` (`engine::msg::time`) is `pub`: the slot and the stamp of each spin tick.
  Merit: it is vocabulary of `msg`, the data that crosses domains (root comment of
  `engine/src/lib.rs`). It states the stamp that each `SpinTick` carries. The timing harness names
  `engine::msg::time` (`tools/latency-probe/clock.rs`, `tools/latency-probe/rig.rs`).
- [ ] `Preflight`, `StartPlan`, `Engine::start`, the pin parameter of `Engine::start`, and the venue
  port trait that `Preflight::run_with` takes are `pub` by a ruling of §12 (2026-11-13 ruling on
  the start seam, in the section on seams that change). The engine binaries of package
  `strat-micro-recorder` reach them only through `run_trading_engine`
  (`strategies/strat-micro-recorder/te-binance-spot-btcusdt/main.rs`,
  `strategies/strat-micro-recorder/te-polymarket-btc-updown-5m/main.rs`). The pin call
  `pin_to_core` keeps its merit: the timing harness passes it (`tools/latency-probe/rig.rs`,
  `tools/latency-probe/idle.rs`, `tools/latency-probe/sender.rs`).
  - Replaced 2026-11-13: "`Preflight`, `StartPlan`, `Engine::start`, the pin parameter of
    `Engine::start`, and the venue port trait that `Preflight::run_with` takes are `pub`. Merit:
    the engine binaries of package `strat-micro-recorder`, another compile root, start the engine
    through them (`strategies/strat-micro-recorder/te-binance-spot-btcusdt/main.rs`,
    `strategies/strat-micro-recorder/te-polymarket-btc-updown-5m/main.rs`)." With its line
    "Amended 2026-11-13, as built: the binaries start the engine through `run_trading_engine`,
    which runs `Preflight` and `Engine::start` in the crate. The timing harness passes the pin call
    `pin_to_core` (`tools/latency-probe/rig.rs`, `tools/latency-probe/idle.rs`,
    `tools/latency-probe/sender.rs`)." WHY: the binaries call only `run_trading_engine`, so the
    merit that the clause named did not hold as built.
- [ ] `Exit`, the decision that gives it, the forced-exit parameter, and `FatalReason` are `pub`.
  Merit: the same binaries give the forced-exit function and map `Exit` to the exit code.
  `FatalReason` is the value of an `Exit` that halted.
- [ ] Each other item of these modules keeps its visibility. Two seams have no caller outside the
  tests: the drain watchdog and the exit outcome of the session chassis. §12 rules on them
  (2026-11-13 ruling in the section on seams that change).
- [ ] Amended 2026-11-13, as built: each other new `pub` item, with its merit:
  - `VenueTs`, `LocalTs`, `FoldTime`, and their named methods: vocabulary of `msg`. A strategy
    crate calls `as_venue_calendar`
    (`strategies/strat-micro-recorder/te-polymarket-btc-updown-5m/strategy.rs`).
  - `Notional`, `Spend`, and `from_mantissa` and `mantissa` of `Price`, `Qty`, and `Notional`:
    vocabulary of `msg`. The strategies and the timing harness call them.
  - `QueueId::new`, its error `QueueIdError`, and `QueueId::index`, which replaces the public field:
    vocabulary of `msg`. The timing harness calls `QueueId::new` (`tools/latency-probe/sender.rs`).
  - `FailureClass` and `Halt` (`engine::msg::failure`): vocabulary of `msg` (§6).
  - `MessageHeader`, `MessageBody`, `MessageKind`, and `InboundMessage::kind`
    (`engine::msg::inbound`), `StageSample::ABSENT_VENUE` (`engine::msg::latency`), and the
    builders of the two chunks, `BookChunk::new`, `BookChunk::push_levels`, `AccountChunk::new`,
    and `AccountChunk::push_balances`: vocabulary of `msg`, the data that crosses domains. The
    timing harness builds each message from a `MessageBody` (`tools/latency-probe/rig.rs`). A
    chunk keeps its entries and its length private, and its filled part is read through
    `active_levels` and `active_balances`, which were `pub` before.
  - `UiHalt` with `is_halted` (`engine::msg::ui`), the kill switch as a workstation reads it, its
    halt stamp a `VenueTs` that the writer converts with `to_peer()`: vocabulary of `msg`, the data
    that crosses domains. The dom fixture, another compile root, builds it
    (`tools/dom-fixture/shell_scene.rs`, `tools/dom-fixture/monitor_scenes.rs`).
  - `SpinSchedule::slot_to_fire`, with the type of the clause above. `StateDigest`, the value of
    `state_digest`.
  - `PinCall` and `pin_to_core`: the timing harness passes `pin_to_core`.
  - `HotLoop`, `PinnedHotThread`, and `touched_input_ring` (`engine::hot::spawn`): the timing
    harness starts its hot thread through them (`tools/latency-probe/rig.rs`).
  - The failure class of each lane, as a constant beside its type: `QueueProducer::WHEN_FULL` and
    `LinkQueueProducer::WHEN_FULL` (`engine::hot::spawn`), and `DropCountingSink::WHEN_FULL`,
    `StrategyLogSink::WHEN_FULL`, `SampleSink::WHEN_FULL`, `ExecSink::WHEN_FULL`,
    `PersistSink::ROW_WHEN_FULL`, `PersistSink::SEAL_WHEN_FULL`, `PersistSink::SEAL_PAST_CAP`, and
    `ExposureSink::WHEN_FULL` (`engine::hot::sink`). Merit, for the group: the rule of §6 that each
    lane type names its failure class in code, beside the type (2026-11-13 section on failure
    classes). The sinks sit in the concept module `engine::hot::sink`, and the two input producers
    in `engine::hot::spawn`.
    - Amended 2026-11-18, as built: three input producers name their class:
      `QueueProducer::WHEN_FULL`, `LinkQueueProducer::WHEN_FULL`, and
      `ControlQueueProducer::WHEN_FULL` (REJECT). Each is defined in the concept module
      `engine::hot::ingress`, and `engine::hot::spawn` re-exports them, so the paths above hold.
      WHY: the write ends moved beside the read end of the input queues, one concept in one file.
  - `StrategyGate`: `StrategyCtx::gate` returns it to strategy code, the seam of §8 (2026-07-20).
  - `Ema` (`engine::hot::series`): the series module is a concept in its own right, and the
    strategies name it (`strategies/strat-micro-recorder/te-binance-spot-btcusdt/models.rs`).
  - `FastQueue::ema_over_window` and `FastQueue::median_over_window` (`engine::hot::series`), the
    new names of `ema` and `median`, and `FastQueue::running_mean`, which takes the place of `mean`
    for a queue of `f64`: the strategies call them
    (`strategies/strat-micro-recorder/te-binance-spot-btcusdt/strategy.rs`, `models.rs` beside it).
  - `DrainOutcome`, `Trigger`, `DrainFailure`, `DrainDeadline`, `ForcedExit`, `AnswerPart`, and
    `SweepFailure`: the inputs and the values of `Exit` and `FatalReason`, by the clause on `Exit`.
  - `DrainWatchdog` and `ExitPlan`: by the ruling of §12 (2026-11-13).
- [ ] A fitness test that names these follows from this ruling. It is never the argument for it.

### Amended 2026-11-13: one engine reads several sources

WHY: a strategy can trade on or read more than one venue. A TE read one source (2026-07-25), so
each venue needed its own engine. The link between two engines carries state and drops frames
(§6, 2026-07-25). Thus no fold saw the messages of two venues in one order.

- [ ] A TE reads one source or more. Its config names them in the list `sources:`. Each source has
  its own execution block and its own checks.
  - Amended 2026-11-13, as built: each item of `sources:` holds `feed:` and its own optional
    `execution:` block. No `execution:` block stands at the top of the config. The config refuses
    (HALT, start refused): an empty list (`ConfigError::NoSource`), two sources of one venue with
    another venue between them (`VenueSourcesApart`), two execution blocks for one venue
    (`VenueExecutionRepeated`), and two sources that send orders in different modes
    (`SourceModesDiffer`). WHY: one venue has one account, so one block sets its execution.
  - Amended 2026-11-13, as built: the sources of one venue share one `VenueId`. Thus a shared asset
    of that venue has one balance row. WHY: a venue has one execution edge and one account, so its
    sources share its id.
- [ ] Identity stays two-part (2026-07-25). One TE = one binary = one process. A source is an input
  of a TE, never a part of its identity.
- [ ] Each venue that a TE trades on has its own execution edge, command lane, execution lease, and
  execution input queue. Each adapter keeps its ONE input queue (2026-08-08).
  - Amended 2026-11-13, as built: the registry gives each venue that sends orders its own execution
    input queue. The start still drives one execution edge with one lease: preflight refuses a
    second source that sends orders (`StartError::ExecutionOnSeveralSources`). Stage 3 tasks T6
    and T12 of the master plan build the start of several edges with their leases. Then this
    refusal goes. WHY: the plan defers the start of several edges first when the part is too large.
  - Amended 2026-11-14, as built: preflight also refuses, before any venue read, an armed config
    whose registry holds rows of more than one venue (`StartError::ExecutionWithSeveralVenues`),
    until stage 3 tasks T6 and T12 of the master plan. WHY: the start gives the one execution edge
    each row of the registry, so it cannot quote the other venue, and the fold panics at its first
    event.
  - Amended 2026-11-14, as built: the exposure file keys each row by (venue, market, symbol), in
    version 3. WHY: two markets of one venue can trade one symbol, as Binance spot and perpetual
    BTCUSDT do.
- [ ] Preflight runs the checks of each source (§2, 2026-11-13). The execution leases stay the last
  checks. The one identity lock of the engine comes before each lease, in each mode, and a lease
  holds no lock. Thus a refused lock leaves each nonce unspent.
  - Amended 2026-11-18, as built: a lease holds the run nonce of its venue only (§2, 2026-11-13
    section on preflight). The execution lease of each venue in the list above is that nonce. WHY:
    one lock for each engine, in each mode, keeps a second process of the identity out, also with
    execution off.
  - Replaced 2026-11-18: "Preflight takes the lock of each lease before it advances any run nonce.
    Thus a refused lock leaves each nonce unspent." WHY: the lock left the lease.
  - Amended 2026-11-13, as built: preflight runs the checks of each source, and it takes one lease.
    The order of several locks and nonces comes with the start of several edges (stage 3 tasks T6
    and T12 of the master plan). WHY: until then one source sends orders.
- [ ] The cap of 20 input queues (§2) counts the queues of all sources. The start refuses a config
  that needs more (`ConfigError::TooManyInputQueues`).
- [ ] The execution state of each venue sits in its own `VenueExec`. Examples: its account, rate
  budget, readiness, reject streak, and command bank. A field that only the messages of one venue
  change belongs there.
  - Amended 2026-11-13, as built: `VenueExec` holds the account, the command bank and its lane, the
    readiness, the reject streak, the rate budget, the settings, the silence of the order stream,
    the count of reconcile passes, and the run nonce of its venue. A stream reset invalidates the
    slots and the readiness of its own venue only. The order tables, the refusal latch, the loss
    sums, and the incident counts stay engine-wide. `Registry::venue_name` is the one source of the
    name of each venue. WHY: a halt and the loss budget stop the whole engine, and an order slot
    names its venue through its instrument.
- [ ] The registry knows an asset by its venue and its name. Thus USDT at two venues is two assets
  with two balance rows.
- [ ] A halt, the spin, and the fold time stay engine-wide. A quoting halt stops each venue of the
  engine.
- [ ] Each message that changes the state of one venue names that venue, directly or through its
  instrument. The fold reads the venue from the message, never from its input queue. Hot state is
  a function of the messages (§2).
- [ ] Fitness tests pin it. `two_venues_equal_two_single_runs`: one run of two venues gives the rows
  of two runs of one venue each. `balances_of_two_venues_never_mix`: two venues never share a
  balance.

### Amended 2026-11-13: the execution contract of part 3

WHY: three faults had no end. An order that the venue never answered got status probes with no
end. A dead order stream never escalated while its reads answered. The signed REST client of
Binance sent an order action again after a lost answer.

- [ ] No resend. An order action that can have reached the venue is never sent again (2026-08-08:
  reconcile-never-resend). The edge and the fold expire an unanswered request by one timeout,
  `execution.inflight_timeout_ms`. The start refuses a venue whose order client has an HTTP timeout
  at or above it (`StartError::ExecutionHttpTimeout`).
  - The signed REST client of Binance returns a failed order action at once. Only a read is tried
    again. Pin: `a_timed_out_order_action_is_sent_once`.
  - Amended 2026-11-14: this holds after any failure, the venue codes -1006 and -1007 ("execution
    status unknown") among them. Pin: `an_order_action_whose_outcome_is_unknown_is_sent_once`. WHY:
    an answer of "status unknown" leaves the order action perhaps done, as a lost answer does.
  - Amended 2026-11-16, as built: -1006 and -1007 answering a cancel or an amend are
    `Ambiguous` on both paths, so the edge reads the order first. A read that fails, or that
    answers -1006 or -1007, re-arms the cancel. Section on the execution contract of part 4, end
    of §8.
  - Replaced 2026-11-16: "so the edge reads the order before any second cancel or amend", and the
    words "and only -1021 re-arms the cancel" in the WHY of the record below. WHY: a read that
    fails, or that answers -1006 or -1007, re-arms the cancel too (the review of part 4, F3).
  - Replaced 2026-11-16: "Amended 2026-11-14, as built: after a failed REST cancel, the edge reads
    the order back at once only at 429 and at a transport failure. At -1006, -1007, and -1021 it
    re-arms the cancel. The read then comes from the in-flight timeout of the hot slot, and an exit
    sweep can send the same cancel again. Part 4 takes this behavior, with question Q1 of the
    review. WHY: no money moves, because a second cancel of one client order id adds no order."
    WHY: part 4 took the behavior. -1006 and -1007 leave the cancel perhaps done, so the edge now
    reads the order first.
  - Replaced 2026-11-14: "A REST cancel that fails, at -1021 or 429 too, then reads the order
    back." WHY: the edge does not act so (the review of part 3, round 2).
- [ ] Probe cap. A slot counts its status probes. At `execution.max_status_probes` the instrument
  gets one read of open orders. IF the order stays unknown after that read, THEN quoting halts
  (`HaltReason::StatusProbesSpent`).
  - Amended 2026-11-14: a venue event that resolves the slot to a state other than `Unknown` sets
    its count to 0, so the cap counts one unanswered episode. Pin:
    `answered_timeouts_spend_no_probe`. WHY: each stream reset gives each working order a timeout,
    so a count that never fell halted an order that the venue answered.
- [ ] Silence escalation. IF the order stream shows no event for `execution.stream_silence_ms` of
  message time while an order works, THEN one read of open orders goes out.
  - A read that agrees with the order table ends the silence, and no count rises.
  - A read that differs is one missed event: the fold takes the state of the read and sends
    `ExecCommand::ResubscribeStream` to the edge.
  - IF the missed events pass `execution.max_stream_resets` with no stream event between, THEN
    quoting halts (`HaltReason::StreamSilent`). Only an event of the stream sets the count to 0.
  - Thus a quiet stream with a resting order never halts.
- [ ] Incident records: §6 (2026-11-13 section on money).
- [ ] One generic seam suite pins the nine obligations of `engine/src/venues/exec/README.md`: the
  macro `venue_edge_keeps_each_written_obligation!` gives one test for each obligation. Each venue
  edge runs it.
  - Amended 2026-11-16, as built: three edges run the suite: Binance, Polymarket, and the simulator
    (`venues::binance::obligations`, `venues::polymarket::obligations`, `venues::sim::obligations`).
    Section on the execution contract of part 4, end of §8.
  - Replaced 2026-11-16: "Binance runs it today, and part 4 adds the other venues." WHY: part 4
    added them.

### Amended 2026-11-13: the `pub` items of part 3, each with its merit

WHY: §12 (2026-07-28) forbids `pub` for a test's sake. Each item below has a caller that is not a
test, sits in a module that is a concept in its own right, or has a ruling of §12.

- [ ] Concept module `engine::hot::exec`:
  - `ExecCapacity` with its sizes, `command_bank` among them: the start code and the timing
    harness size an engine from the same rows.
  - The named transitions of `OrderTable`, the getters of `OrderSlot`, `ClaimRefusal` (`IdsSpent`,
    `LevelFull`), the field `OrderClaim.run_nonce`, and `OrderTable::slots_in_use`.
  - `ExecVenueSetup` and `ExecVenueSetup::disabled`: the element of the field
    `ExecEngineSetup.venues`, which replaces its fields `run_nonce`, `settings`, and `sink`.
  - The `ExecSettings` fields `max_status_probes`, `stream_silence`, and `max_stream_resets`.
  - `HaltReason::{OrphanFill, PriorRunOverflow, StatusProbesSpent, StreamSilent, WriterFailed}` and
    `RejectReason::IdsSpent`: the reasons cross to the link and the workstation.
    - Amended 2026-11-14: `HaltReason::PriorRunFillUnbooked`, by the same merit. WHY: the review of
      part 3 added it.
- [ ] Vocabulary of `msg`, the data that crosses domains: `VenueId`, `ExecEvent.venue`,
  `AccountChunk.venue`, `IncidentRow`, `IncidentKind`, `PersistRecord::Incident`,
  `ExecCommand::ResubscribeStream`, `MessageBody::WriterFailed`, `MessageKind::WriterFailed`,
  `WriterFailure`, `FileWriter`, and the `EngineCounters` fields `fee_gaps`, `stranded_positions`,
  `orphan_fills`, `unbooked_fees`, and `orphan_events_without_fill`. The metrics report reads the
  counters.
- [ ] Concept module `engine::venues::exec`: `ExecSessionError` with `failure_class` and
  `fatal_reason`, `ExecSession::undecodable_open_order`, `ExecCore::foreign_orders` (the chassis
  writes the count when a session stops), `OrderOwnership` as an enum, and the removal method of
  `InFlightTable` with `SessionSetup.inflight_timeout`.
- [ ] Concept module `engine::runtime::config`: `Config::sources` with `SourceConfig`, the new
  `ExecutionConfig` fields, `ExposureConfig::max_failed_writes`, and the new `ConfigError`
  variants. The binaries load the config.
  - Amended 2026-11-14: `Config::execution_mode`, the mode of the run, is `pub` because the timing
    harness calls it (`tools/latency-probe/config.rs`). Each other new getter of `Config`,
    `SourceConfig`, and `AssetDictionary` is `pub(crate)`, or gone. WHY: the check of part 3 found
    these getters `pub` with no merit in this list.
- [ ] Concept module `engine::runtime::registry`: `InstrumentRow.venue`,
  `Registry::writer_status_queue_id`, `Registry::venue_name`, and `Registry::asset_symbols`. The
  other getters of `Registry` are `pub`, so one design language holds.
- [ ] Concept module `engine::runtime::exposure`: `ExposureSnapshot::{for_instruments, active,
  set_rows, copy_from}`.
- [ ] `failure_class` of `PersistError`, `ExposureError`, `SweepFailure`, and `DrainFailure`: the
  rule of §6 that each error enum that reaches a supervisor has it.
- [ ] `Exit::refused`: by the clause on `Exit` (2026-11-13 section on the fold, start, and exit
  seams).
- [ ] `StartError::{ExecutionCapacity, ExecutionNonceEmpty, ExecutionHttpTimeout,
  ExecutionOnSeveralSources}`: `StartError` is `pub` by the ruling of §12 on the start seam.
  - Amended 2026-11-14: `StartError::ExecutionWithSeveralVenues`, by the same ruling. WHY: the
    review of part 3 added it.
- [ ] `HotEngineSetup.exec`, now one `ExecWiring` for each venue: the timing harness builds
  `HotEngineSetup`.
- [ ] By the rulings of §12 (2026-11-13): the fault seam of the chassis (`halt_on_session_error`),
  the loopback constructor `SignedRestClient::on_loopback`, and the value form of the exposure
  writer seam.
- [ ] A fitness test that names these follows from this ruling. It is never the argument for it.

### Amended 2026-11-13: the OTLP export is `pub` on merit

WHY: §12 (2026-07-28) forbids `pub` for a test's sake, so each new item names its merit.

- [ ] `engine::runtime::otlp` is a concept in its own right: the OTLP export of a run. Its items
  `OtlpPlan`, `OtlpLanes`, `OtlpExporter`, `OtlpRows`, `OtlpReport` and `OtlpError` are `pub` on
  that merit. `OtlpRows::WHEN_FULL` is `pub` by the rule of §6 that each lane type names its class.
- [ ] `StartError::Otlp` is `pub` by the §12 ruling on the start seam (2026-11-13). `OtlpConfig` and
  `Config::otlp` are vocabulary of `engine::runtime::config`.
- [ ] The copy lane of the log and the sink parameter of the metrics thread stay crate-private.
- [ ] Package `engine` has feature `otlp` beside `mcp` and `web` (2026-11-12 section on two lib
  crates). It is native only, and `--all-features` of the gate builds it.
- [ ] A fitness test that names these follows from this ruling. It is never the argument for it.

### Amended 2026-11-15: the execution contract of part 4

WHY: the Polymarket edge adopted an unknown open order that matched no own order exactly. It bound
that order to the first lost slot of its side. That order can belong to a person. A cancel of the
slot then touches an order that this engine did not place. The fold also books the fills of that
order as its own.

- [ ] Adoption by exact match. A venue can mint the id of each order, as Polymarket does. Then a
  read can show an open order with an id that the edge never recorded. The edge gives that order
  one verdict (`classify_unmapped`, `engine::venues::polymarket::exec::correlate`):
  - IF a placement of this run on its instrument and side still waits for its answer, THEN
    `Defer`. That answer can still name the order.
  - ELSE IF exactly one lost slot equals the order in instrument, side, price, and original size,
    THEN `Adopt`. That slot takes the venue id of the order.
  - ELSE `LeaveAlone`, also when two or more lost slots equal the order. The edge never mirrors and
    never cancels it. It counts one foreign order (`ExecCore::foreign_orders`), and the chassis
    writes the count when the session stops.
- [ ] Lost slot: an order that this run placed, with no venue id. Its placement timed out, or its
  transport failed, so no place answer will come. A lost slot that no adoption binds stays
  ambiguous. The probe cap ends it (2026-11-13 section on the execution contract of part 3).
- [ ] Pin: `order_with_no_exact_match_is_never_adopted`.
  - Amended 2026-11-16, as built: the resync of the Polymarket edge counts each `LeaveAlone`
    order with `ExecCore::count_foreign_order`. No test reaches that call, because the actor runs
    only inside a live edge. The verdict has its pin above, and the count of the core has the pin
    `foreign_order_is_never_mirrored_or_cancelled`. WHY: the count is the one trace of an order
    that this engine did not place.
- [ ] Amended 2026-11-16: the first read of a token. The edge never cancels an open order that it
  cannot prove its own. WHY: Polymarket echoes no client id, so at a start the edge cannot tell
  its own order of an earlier run from the order of a person, which the website places with the
  same credentials. A cancel can touch an order that this engine did not place (third Never
  line). Quoting beside it risks fills that the fold cannot book, and exposure outside the caps
  (§6, money).
  - First read of a token: each read of the open orders of the token, from its binding at a start
    or at a rotation, until its first read ends. The first read ends only at a fresh-binding read
    of that token, while the token is still bound, in which no unknown order got `Defer`.
  - A `Defer` keeps the first read open: the order waits for the answer of a placement of this
    run. The answer of the venue to a placement on that instrument, accepted or refused, asks for
    one more read of the token, and that read is still a first read.
  - IF a first read names an open order on the token, and neither `Defer` nor `Adopt` applies,
    THEN the order is unproven. The edge counts it and never cancels it, and quoting halts with
    the halt reason of an unproven order. A person decides.
  - A read of a token that is already quoted keeps the verdicts above.
  - An open order on a token that no binding names is left alone and counted. It halts nothing,
    and the edge never cancels it.
  - Pin: `a_cold_start_read_naming_an_unknown_order_sends_no_cancel`.
  - Amended 2026-11-16, as built: `classify_unmapped` takes the kind of its read, `TokenRead {
    First, Quoted }`, and gives `UnmappedVerdict::Unproven` at `First`, where it gives
    `LeaveAlone` at `Quoted`. `unproven_order_event` builds the message of an unproven order
    (`engine::venues::polymarket::exec::correlate`): kind `ExecKind::UnprovenOrder`, no client
    id, the venue id digest, side, price, and size, and the received stamp of its read. The edge
    writes one ERROR line with the venue id, side, price, and size, and the fold halts with
    `HaltReason::UnprovenOrderResting` (§6). The edge has no request that cancels by a venue id
    alone: the prior-run cancel and its wait of readiness are gone. WHY: a reader finds each
    part of the rule by its name in the code.
  - Amended 2026-11-16, as built: `FirstReads` (`engine::venues::polymarket::exec::correlate`)
    holds the tokens whose first read has not ended. `on_bound` makes it due at a binding,
    `read_of` gives the `TokenRead` of each unknown order, `on_read_answered` ends it at a
    fresh-binding read with no `Defer`, and `take_read_after_answer` asks for the one more read
    at a placement answer. A fresh-binding read that its lane drops leaves the first read due, so
    each later read of the token is a first read too, until its next binding: an error toward a
    halt. Pin: `a_first_read_that_defers_stays_first_until_the_placement_answers`. The calls of
    the actor have no pin, because the actor runs only inside a live edge: the binding, the read
    kind of each unknown order, the check that the token is still bound, and the read after the
    answer. WHY: a pass, or a read of a token since rebound, can miss the orders of the token that
    is bound now, so only the fresh-binding read of that token can end its first read.
  - Replaced 2026-11-16: "First read of a token: the first pass of a run, and the first read of
    each token that a rotation binds." WHY: a first read that deferred an order ended at that
    read, and no later read of the token was a first read. So the edge left the order alone and
    quoted beside it, at the binding of a token, where an order of an earlier run of this engine
    rests (the review of part 4, round 2, F6).
- [ ] Amended 2026-11-16: the heartbeat deadline. WHY: Polymarket cancels each order of the account
  when no heartbeat reaches it within its limit. An edge that kept its mirror past that limit would
  quote from orders that no longer rest.
  - IF a heartbeat fails in transport, is refused, or names no id, THEN it is due again one retry
    interval after it went out. Thus a venue that refuses each beat hears at most one a second,
    and three tries come before the deadline.
  - IF no heartbeat is answered for the venue limit less a margin, from the last answered one, THEN
    the edge drops its connection. The path of a lost connection rebuilds the order mirror, and
    quoting stays closed until a heartbeat is answered.
  - IF the venue limit itself passes, THEN a new resync pass reads the open orders again.
  - The period, the limit, the margin, and the retry interval are facts of the venue
    (`PolymarketFacts`): 5 s, 10 s, 2 s, and 1 s (`heartbeat_retry`) at Polymarket.
  - Three request lanes keep a cancel and a heartbeat from waiting behind a held placement.
  - Pins: `lost_heartbeat_past_deadline_resets_order_mirror`, which polls the schedule at each
    20 ms tick of the edge, and `cancel_and_heartbeat_finish_while_a_place_blocks`.
  - Replaced 2026-11-16: "THEN it is due again at the next poll." and "The period, the limit,
    and the margin are facts of the venue (`PolymarketFacts`): 5 s, 10 s, and 2 s at
    Polymarket." WHY: the next poll is the next tick of 20 ms, so a venue that refused each beat
    got one signed beat at each tick, up to 50 a second (the review of part 4, F2).
- [ ] Amended 2026-11-16, as built: the window-close sweep cancels only the orders of this run.
  WHY: near the close of a window, the backstop of the Polymarket edge sent
  `DELETE /cancel-market-orders` for the token. That request cancels each order of the account
  on the token, a person's order among them (third Never line).
  - The backstop cancels each order of this run on the closing window, one at a time, by its
    own id (`ExecSession::sweep`).
  - No edge sends a bulk cancel of its venue, such as a cancel of each order on a token. The
    codec has no encoder of one: `cancel_market_orders` and its request are gone.
- [ ] Amended 2026-11-16: the named refusal of the simulator. WHY: the simulator answered an order
  that it cannot model with a filter failure of the venue (-1013). Thus a log or a tape could not
  tell a limit of the simulator from a refusal of the venue.
  - The simulator refuses each order of a style that it cannot model, an `Immediate` order today,
    with code 9,001 and a message that names the style. No venue sends that code.
  - The codec of Binance gives code 9,001 its own arm, class `Fatal`. Thus quoting halts with
    `HaltReason::FatalReject` at the first refusal, as at -1013.
  - The run summary of the simulator counts the refused styles.
  - Pin: `unmodelled_style_gets_a_named_simulator_refusal`.
- [ ] Amended 2026-11-16, as built: the unknown outcome of a Binance cancel or amend. WHY: an
  answer of "execution status unknown" leaves the cancel or the amend perhaps done. A second action
  before a read can act on a state that the edge does not know.
  - The codec gives -1006 and -1007 the class `Ambiguous` on a placement, a cancel, and an amend.
    A status query stays `StillLive`, because a read changes nothing.
  - WebSocket API: the edge sends one read of the order, and the cancel latch stays shut until the
    read answers. IF the read itself answers -1006 or -1007, THEN the read is `StillLive`, and
    the edge re-arms the cancel.
  - REST: after a cancel of class `Ambiguous`, the edge reads the order back at once, and the
    cancel latch stays shut. A REST cancel is `Ambiguous` at -2011, -2013, -1006, and -1007, at
    HTTP 429 or 418, at an HTTP 5xx with no code of the venue, and at a failure with no answer
    of the venue. IF that read fails, THEN the edge re-arms the cancel.
  - Binance gives an HTTP 5xx as an execution status unknown. Thus a REST cancel answered 5xx
    with no code can have run, as at -1006. A 5xx with a code keeps the class of that code.
  - At HTTP 429 or 418 the REST client reads no code: it gives `RestError::RateLimited`, the
    rate limit of the venue, and the edge classes it `Ambiguous`. Binance sends -1003 and -1015
    with these statuses. Thus over REST a cancel that a rate limit refused reads the order back.
  - A cancel answered `StillLive` re-arms the cancel: -1021 on both paths, and -1003 and -1015
    over the WebSocket API only, where the codec reads the code of each answer. The gateway
    refused the request before the match engine, so the cancel did not run, and a later cancel
    is a new action.
  - Thus a second cancel can go out, from the next cancel or from the exit sweep, while the
    outcome of the first is still unknown. No money moves, because a second cancel of one client
    order id adds no order.
  - Replaced 2026-11-16: "WebSocket API: the edge sends one read of the order. No second cancel
    or amend goes out before that read answers.", "REST: after a failed cancel, the edge reads
    the order back at once at -1006, -1007, 429, and a transport failure. The cancel latch stays
    shut until the read answers.", and "-1021 stays `StillLive`, and only -1021 re-arms the
    cancel." WHY: a read that fails, and a read that answers -1006 or -1007, re-arm the cancel
    too, and so does each cancel answered `StillLive` (the review of part 4, F3).
  - Replaced 2026-11-16: "REST: after a cancel of class `Ambiguous` (-2011, -1006, -1007, or no
    answer), the edge reads the order back at once, and the cancel latch stays shut." and "A
    cancel answered `StillLive` (-1003, -1015, -1021) re-arms the cancel." WHY: at HTTP 429 or
    418 the REST client reads no code, so over REST -1003 and -1015 read the order back, and
    only the WebSocket API re-arms the cancel at them (the review of part 4, round 2, F7).
  - Pins: `an_unknown_outcome_of_a_cancel_over_the_websocket_reads_before_any_second_cancel`,
    `an_unknown_outcome_of_a_rest_cancel_reads_before_any_second_cancel`, and
    `a_rest_cancel_answered_5xx_with_no_code_reads_before_any_second_cancel`.
  - The class of a failed REST cancel is set in the private actor of the Binance edge, which runs
    only inside a live edge. The REST pins hold the client and the core on each side of that
    step, and no test drives a REST cancel that a rate limit refused.
- [ ] Amended 2026-11-16, as built: the seam suite runs for the Polymarket edge and the simulator
  edge too. WHY: one suite for each edge holds the written obligations, so a new edge cannot keep
  only some of them.
  - A venue that sends no balance delta asserts only the absolute chunks, and a venue with no
    pending cancel asserts only the finality of the cancel answer.
  - The Polymarket edge halts at a page of open orders that it cannot decode, as Binance does
    (`ExecSessionError::UndecodableOpenOrder`).
  - The events of the Binance and Polymarket stand-ins pass through an `EventFunnel` in front of a
    real ring. The simulator stamps its own queued times, so for it that half stays a named gap.

### Amended 2026-11-15: the interface of the simulated venue is `pub` on merit

WHY: §12 (2026-07-28) forbids `pub` for a test's sake. The simulated venue is a concept in its own
right. Its orders and its wallet were `pub`, and the venue that moves them was not.

- [ ] `SimVenue` (`engine::venues::sim::core`) is `pub`, with `new`, `on_request`,
  `on_market_batch`, `advance_to_watermark`, `restore_matching`, `suspend_matching`, `wallet_mut`,
  and `unmatched_prints`. Each type that this interface names is `pub` with it: `SimVenueSetup`,
  `SimEmission`, `VenueEvent`, `OrderSnapshot`, `OrderPhase`, `ClosedReason`, `RefusalReason`,
  `InstrumentLimits`, `QueueAhead`, `ResetReason`, `LatencyBudget`, `UnmatchedPrints`, and
  `SimEdgeError`.
  - Merit: `engine::venues::sim::core` is `pub` by an earlier decision. It is the matching model
    that the tests of the live Binance codec use as their corpus (`engine/src/venues/sim/mod.rs`).
    `SimVenue` is that model. Its modules `orders` and `wallet` are `pub` on the same merit.
  - `SimVenue::market` and `SimVenue::force_exit_open_orders` stay `pub(crate)`. Only the actor and
    the driver of the simulator call them.
- [ ] `SimHorizon` with `unseeded`, `observe`, `has_all_watermarks`, and `safe_venue_horizon`, and
  `SimLane` (`engine::venues::sim::lanes`) are `pub`. Merit: the horizon is the half of the
  interface that decides when the venue can advance. `LaneBuffer` and `LaneBufferFull` stay
  crate-private. They are the command lane of the actor, and `SimEdgeError::CommandLaneFull`
  carries its capacity.
- [ ] The properties of the simulator go through this interface:
  - the fill model: `own_fill_never_exceeds_print_and_never_jumps_the_queue`,
    `trade_id_gap_suspends_matching_until_snapshot`, `unmatched_prints_are_counted_by_reason`
  - the order of the lanes: `lane_arrival_order_never_changes_fills`,
    `nothing_is_applied_past_the_horizon`
  - the full inbox: `full_simulator_inbox_halts_the_run_and_evicts_nothing`
  - the seam suite for the simulator edge (2026-11-13 section on the execution contract of part 3)
- [ ] Rejected: tests inside the crate. §12 (2026-07-28) forbids a `#[cfg(test)]` block outside
  `<package>/tests/`.
- [ ] A fitness test that names these follows from this ruling. It is never the argument for it.

### Amended 2026-11-16: the simulator of part 4

WHY: the simulator checked its config a second time at start with `expect`, a full market inbox
panicked, and it dropped prints with no count. Each one hid a fault of a simulated run until the
run met it.

- [ ] Proofs from config. Config validation proves each fact that a simulated run needs, one time,
  and keeps each proof in the validated config:
  - `SimSupport`: the source can be simulated.
  - `SimLatencyBudget`: the worst round trip of the simulator is below
    `execution.inflight_timeout_ms`.
  - the maker fee as a `FeeBps`, through its one conversion, `FeeBps::from_bps`.
- [ ] Preflight hands the proofs to the plan of the simulator. The plan finds the trades queue and
  the depth queues of the source. IF one is missing, THEN preflight refuses the start with
  `SimVenueError::LaneMissing`, before the lease. The start of the simulated venue takes the
  proofs, so a start with no proof does not compile.
  - Pins: `sim_mode_on_source_with_no_simulator_is_refused_by_name`,
    `round_trip_at_or_above_timeout_refuses_start`, `maker_fee_of_10_bps_charges_known_amount`.
- [ ] A full market inbox is the typed error `SimEdgeError::InboxFull { capacity }`. The venue
  evicts nothing, and the run halts (§6, 2026-11-16). Pin:
  `full_simulator_inbox_halts_the_run_and_evicts_nothing`.
- [ ] The simulated venue counts each print that it does not match, by its reason
  (`UnmatchedPrints`):
  - matching is suspended
  - the print is at or before the last depth commit
  - its ids are ones that its stream epoch has passed
  - it is the first print of an epoch
- [ ] These counts and the prints that the venue walks sum to the prints.
  `SimVenue::unmatched_prints` gives them, and the run summary prints them beside the count of
  refused styles. Pin: `unmatched_prints_are_counted_by_reason`.

### Amended 2026-11-16: the `pub` items of part 4, each with its merit

WHY: §12 (2026-07-28) forbids `pub` for a test's sake. Each item below has a caller that is not a
test, sits in a module that is a concept in its own right, or has a ruling of §12.

- [ ] By the rulings of §12 (2026-11-15):
  - the fault seam of the market-data feeds: `engine::venues::edge::fault`, with `MarketDataError`
    and `halt_on_market_data_error`
  - the fault seam of the simulator edge: `halt_on_sim_error`
  - the request lanes of Polymarket (`engine::venues::polymarket::exec::lanes`) and
    `ClobHttp::on_loopback`
  - the pure gate of the Polymarket start (`engine::venues::polymarket::exec::assess`)
  - the event funnel: `EventFunnel` with `new` (`engine::venues::edge`)
- [ ] By the 2026-11-15 section on the interface of the simulated venue: the interface of
  `SimVenue`, re-exported from `engine::venues::sim::core`, with `SimEdgeError`, and `SimHorizon`
  and `SimLane` (`engine::venues::sim::lanes`).
- [ ] Concept module `engine::venues::polymarket::exec`:
  - `facts`: `PolymarketFacts` and `POLYMARKET_FACTS`, the one value of the facts of the venue. It
    holds the taker hold, the wire margin, the capacity and the lifetime of held frames, and the
    period, the limit, and the margin of the heartbeat. It takes the place of `PENDING_CAPACITY`,
    `PENDING_TTL`, and `DELAYED_HOLD` of `correlate`, which go.
  - `heartbeat`: `HeartbeatSchedule` with `new`, `on_poll`, `on_sent`, `on_answer`, and
    `is_placing_held`, with `HeartbeatStep` and `BeatAnswer`. Pure state with time passed in, as
    `correlate` and `binding` beside it.
  - `assess::ProvenSignatureType` with `code`: the `pub` fields `OrderSignerSetup.signature_type`
    and `WalletIdentity.signature_type` hold it, and only `assess` makes one.
  - `codec::OrderIndex::record_matched`: the codec is the convention surface of the venue, and
    `OrderIndex` is `pub`.
  - `codec::WireError::UndecodableOpenOrder { venue_order_id, source }`: a variant of the `pub`
    error of the codec. It names the first order of a page that the codec cannot decode.
  - `PolymarketPreflightError::LocalClockInvalid { seen }`: a variant of a `pub` error.
    `preflight_polymarket` takes the secrets from its caller (`&EnvFile`).
- [ ] Module `engine::venues::polymarket::rotation`, `pub` by an earlier decision:
  `WindowAssignment.legs` and `ForceTeardownFacts.instruments` take the place of `up`, `down`,
  `up_instrument`, and `down_instrument`.
- [ ] New variants of `pub` enums of `engine::venues::polymarket`:
  `BookFrameOutcome::Suspect { streak }` (`shadow`) and `DriverEffect::Suspect { instrument,
  streak }` (`actor`). The shell of the actor counts the second one.
- [ ] Concept module `engine::venues::sim::wire`, `pub` by an earlier decision: `VenueWire::refusal`
  and the re-export of `Rejection`. Merit: the same as `would_match_error` and
  `unknown_order_error`, the payloads that the live codec must keep reading.
- [ ] `engine::venues::sim`: `SimVenueError::LaneMissing { lane }`, a variant of a `pub` error.
  `SimVenueError::Latency` goes. `RefusalReason::UnmodelledStyle { style }` takes the place of
  `StyleNotPermitted`.
- [ ] `FatalReason::SimulatorInboxFull { capacity }`: `FatalReason` is `pub` (2026-11-13 section on
  the fold, start, and exit seams).
- [ ] Vocabulary of `engine::runtime::config`, which the binaries load: `FeeBps` with `from_bps`,
  the one conversion of a fee. Its old path `engine::venues::sim::core::wallet::FeeBps` stays, by a
  re-export. `FeeBps::new` goes.
- [ ] Concept module `engine::runtime::artifacts`: `RunArtifacts` with `new`, `mode`, `stem`,
  `exposure_path`, `tape_dir`, `footer_mode`, and `console_log_path`. It is the one source of the
  name of each artifact of a run (§7, 2026-11-16 line). `ExecutionMode::artifact_stem`,
  `ExecutionMode::artifact_segment`, and `exposure::file_path` go.
  - Four `pub` items take it in place of the mode: `exposure::load(dir, identity, registry,
    &RunArtifacts)`, `ExposureWriterSetup.artifacts` (the value form of the exposure writer seam,
    2026-11-13 ruling of §12), `OtlpPlan::check(config, &RunArtifacts)`, and `RunMeta.artifacts`,
    in place of `RunMeta.execution_mode`.
- [ ] Amended 2026-11-16, as built: the `pub` items of correction round 1 of part 4.
  - Concept module `engine::venues::polymarket::exec::correlate`, the one decision on an open
    order that the edge never recorded: `TokenRead { First, Quoted }`,
    `UnmappedVerdict::Unproven`, `unproven_order_event`, and the signature
    `classify_unmapped(mirror, unmapped, has_venue_id, read)`.
  - `HaltReason::UnprovenOrderResting`: the reasons cross to the link and the workstation, as
    those of part 3.
  - `ExecKind::UnprovenOrder`: vocabulary of `msg`, the data that crosses domains.
  - `PolymarketFacts::heartbeat_retry`: a field of the one value of the facts of the venue.
  - `codec::cancel_market_orders` goes: no edge sends a bulk cancel (the execution contract of
    part 4, above).
  - WHY: §12 (2026-07-28) forbids `pub` for a test's sake, so each item of the round names its
    merit.
- [ ] Amended 2026-11-16, as built: the `pub` item of correction round 2 of part 4. `FirstReads`
  with `on_bound`, `read_of`, `on_read_answered`, and `take_read_after_answer`: merit, the concept
  module `engine::venues::polymarket::exec::correlate`, where it is pure state beside
  `PendingFrames` and `DelayedOrders`. The class of a failed REST cancel of Binance stays
  private. WHY: §12 (2026-07-28) forbids `pub` for a test's sake.
- [ ] The integration tests of part 4 use only items that were `pub` before them. No item became
  `pub` for them.
- [ ] A fitness test that names these follows from this ruling. It is never the argument for it.

### Amended 2026-11-17: the `pub` items of part 5, each with its merit

WHY: §12 (2026-07-28) forbids `pub` for a test's sake. The control logic of the link sat inside
the private link actor, so a test reached it only through a socket. A test had no way to build
the actor without a hot thread. Each item below has a caller that is not a test, sits in a module that
is a concept in its own right, or has a ruling of §12.

- [ ] `ControlPlane` (`engine::link`, in `engine/src/link/actor/control_plane.rs`) is `pub`, with
  `new`, `on_assertion`, `check_controller`, `pending`, and `SILENCE_LIMIT`. `AssertionVerdict`
  (`NoClaim`, `Accepted`, `Stale`), the value of `on_assertion`, is `pub` with it.
  - Merit: the control plane is a concept in its own right. It is the one place that judges the
    assertion of a controller, and the one place of the dead-man rule. It is pure state with the
    time passed in: it reads no clock and owns no socket, queue, or thread. Thus the link actor and
    a test drive it the same way, as with the link seam of the workstation (2026-10-26).
  - The link actor owns it and pushes each marker that it returns.
  - The `DesiredWriter` takes the place of its `RunControlGate`, and the list of controllers and
    their refused counts join its state (§6, the two 2026-11-17 sections on the run state). It
    keeps this merit through each change.
  - Amended 2026-11-18, as built: `ControlPlane::new(DesiredWriter, &[IpAddr], ControllerLoss)`.
    It also has `push_pending`, `refused_assertions`, and `refused_epochs`, which the link actor
    calls. `desired` and `acknowledged` are `pub` by the ruling of §12 on read accessors
    (2026-11-18): only the fitness support calls them from outside the crate. `AssertionVerdict`
    has five variants: `NoClaim`, `Accepted`, `Stale`, `UnlistedSender`, and `EpochTooHigh`.
    `RefusedAssertions` with `of` and `total` is `pub` with it, as the value of
    `refused_assertions`. WHY: the refusals of part 5 are verdicts of the control plane, and it
    holds the one desired writer, so a test reads both states through it.
  - Replaced 2026-11-18: "`acknowledged` stays `pub(super)`." WHY: it is `pub` since the writer
    handles of part 5.
- [ ] `LinkActor` with `spawn(setup, &Handle)`, and `LinkActorSetup` with its fields, are `pub`
  (`engine::link`, in `engine/src/link/actor/mod.rs`). Each type that the setup names is `pub`.
  New with it: `PeerFeed`, `LinkCountsHandle` with `new` and `load`, and `LinkCounts`.
  - Merit: the link actor is the one edge actor of the link (§2), a concept in its own right. One
    task owns the link socket, the subscriber table, and the sequence gate. Each part that it owns
    is `pub`, so a caller builds an actor on loopback from public parts, with no hot thread.
  - The setup holds the socket that preflight bound (§2, 2026-11-13), as a `std::net::UdpSocket`.
    The actor registers it with the runtime that runs the actor, inside its own task.
  - Amended 2026-11-18, as built: `LinkCounts` holds what the actor refused, lost, or sent since its
    start. The actor publishes it at each announce through `LinkCountsHandle`, for a reader on
    another thread: the metrics thread and a test.
    - Counts: `decode_failed` (with `layout_mismatched`, a part of it), `gate_rejected`,
      `topic_ignored`, `send_failed`, `receive_failed`, `table_full_refused`, `peer_silenced`,
      `refused_epochs`, `history_sent`, `history_failed`, and `history_skipped`.
    - Tables: `refused_assertions` (by IP address) and `senders` (by source address).
    - It holds no count of the drops of the control queue (§6, the marker queue) and no count of
      its own for a refused epoch at decode (§6, the controller).
  - Replaced 2026-11-18: "`LinkCounts` holds what the actor refused or lost since its start, one
    count for each cause." and "Each new cause of part 5 is one more count there." WHY: it holds
    tables and a count of sent frames too, and two causes count elsewhere.
  - When part 5 adds the control queue and the writer handles (§6, 2026-11-17), the setup takes a
    `ControlQueueProducer` (`engine::hot::ingress`) and the `DesiredWriter`
    (`engine::runtime::shutdown`). Each one is `pub` with the setup, on this merit.
    - Amended 2026-11-18, as built: the setup also takes `controllers: Vec<IpAddr>` and `history:
      HistoryRing`, and its field `markers` holds the `ControlQueueProducer`.
  - `Actor`, `Wake`, and `PeerState` stay private.
- [ ] Module `engine::msg::ui::channel` is `pub`, with `ui_channel`, `UiWiring`, `UiChannels`, and
  `UiLifecycle`. Merit: vocabulary of `msg`, the feed that crosses from the hot thread to the link
  actor. The setup of the link actor takes `UiChannels`.
- [ ] By the rulings of §12 (2026-11-17):
  - the file steps of a write (`engine::runtime::durable_file`), with `PersistWriter::through`
    and `PersistWriter::on_disk`
  - the one accept loop of the relay and the fleet server (`engine::operator::accept`)
- [ ] Amended 2026-11-18, as built: each other `pub` item of part 5, with its merit. WHY: §12
  (2026-07-28) forbids `pub` for a test's sake, so each item of the builds names its merit.
  - The writer handles `RunStateHandles`, `DesiredWriter`, `DesiredReader`, and
    `AcknowledgedWriter` (`engine::runtime::shutdown`): they take the place of `RunStateCell` and
    `RunControlGate`, which were `pub`. The `pub` contexts of the venue adapters, the setup of the
    link actor, and `LinkWiring.acknowledged` of the fold hold them.
  - `ControlQueueProducer` with `new`, `push`, and `WHEN_FULL`, and the new methods of
    `LinkQueueProducer` (`push(from, message)`, `with_sender_rate`, `sender_rate`,
    `start_rate_period`, `senders`) with their value `LinkPush`: write ends of the input queues, in
    the concept module `engine::hot::ingress`. The setup of the link actor takes both producers.
  - `ControllerEpoch` with `ZERO`, `MAX`, `new`, `saturating`, `get`, `next`, and `TryFrom<u64>`,
    and its error `EpochOutOfRange` (`engine::msg::run_state`): vocabulary of `msg`, the epoch of
    `RunAssertion`, `Subscribe`, and `Lifecycle`. The workstation builds its epochs with it.
  - `LINK_LAYOUT`, `Envelope.layout`, `LinkDecodeError::{LayoutMismatch, EpochOutOfRange}`,
    `TopicId::HISTORY`, and `LinkBody::History`: the wire of the link, `pub` as the envelope and
    the frame are. `LINK_LAYOUT` takes the place of the version number of the envelope, which was
    `pub`.
  - `SubscriberTable::naming`: one more method of a `pub` type, which the link actor calls.
  - `HistoryRing` with `new`, `record`, `begin_flush`, `next_frame`, and `skipped` (`engine::link`):
    the same merit as `ControlPlane`. It is a pure machine of the link actor, with no socket, clock,
    or thread, and the setup of the link actor takes it.
  - `SenderCounts` with `rows`, `SenderCount`, and `LINK_MAX_SENDERS` (`engine::link`): the value
    of `LinkCounts.senders`, which the metrics thread reads.
  - `Metrics::watch_link_counts`, `SenderDropRow`, and `RollupRow::SenderDrop`: the module
    `engine::runtime::metrics` is `pub`, the timing harness names it (`tools/latency-probe/rig.rs`),
    and its rows go to the OTLP export.
  - `Registry::control_queue_id` and `Registry::tape_status_queue_id`: the other getters of
    `Registry` are `pub`, so one design language holds.
  - The new config fields `QueuesConfig.control_capacity`, `PersistenceConfig.seal_interval_spins`,
    and `LinkConfig.{controllers, sender_rate_per_sec, history_capacity, history_per_flush}`, with
    `LinkConfig::history_capacity()` and `history_per_flush()`: the binaries load the config, and
    each of its fields is `pub`. `PersistWiring.seal_interval_spins`: a field of the wiring of the
    fold, whose fields are `pub`.
  - `UiModel::apply_history` and `ChartModel::apply_history`: the link seam of the workstation
    (2026-10-26 section above).
  - `TapeTable` with `as_str`, `fault_class`, and `Display`, and the field `table` of
    `PersistError::{Io, Parquet, Batch}`: the concept module `engine::tape`. `PersistError` is
    `pub` and names its table.
  - `FileWriter::Tape`: vocabulary of `msg`, a variant of a `pub` enum.
  - `AcceptTries` with `Default`, `FromStr`, and `Display` (`engine::operator::accept`): a caller
    that is not a test, `polysim-mcp`, parses its flag with it. `HttpSettings { port, stdin_watch,
    accept_tries }` and `serve_http(fleet, settings)`: `polysim-mcp` composes them.
  - New variants of `pub` errors: `WebError::Accept`, `ArgumentError::AcceptTries`, and
    `FleetError::{Stopping, RunningElsewhere, LockProbe, ConfigWriteLeftBehind, ConfigNotSynced}`.
- [ ] Amended 2026-11-18: by the rulings of §12 (2026-11-18): `LinkCredential`; the identity locks
  of the start seam (`IdentityLocks`, `IdentityLock`, `Preflight.locks`);
  `Fleet::with_identity_locks`; `Fleet::with_file_steps` and `FileSteps::set_permissions`; the
  fault seam of the tape writer; and the read accessors that only tests call.
- [ ] Amended 2026-11-19: no item of part 5 is `pub` with no merit and no ruling. The correction
  round of the verifier of part 5 removed `PersistError::table()`, `RefusedAssertions::past_table`,
  `SenderCounts::others`, `AcceptTries::new` and `Fleet::stop`, which nothing called, and made
  `Config::seal_interval_spins()` `pub(crate)`, as `seal_try_limit` is. `Fleet::stop_all` keeps
  its merit: `PolysimMcp::stop_all` calls it once, at the exit, after each session has ended, so its
  hold of `&mut Fleet` through the wait holds up no request; a stop of one engine waits outside the
  lock of the fleet. WHY: an item that is `pub` with no caller is a promise to a caller that does not
  exist, and the law should name only interfaces that something uses. Replaced: the line of
  2026-11-18 that listed these items as findings for the verifier.
- [ ] A fitness test that names these follows from this ruling. It is never the argument for it.

## 9. Naming & API Ergonomics

- [ ] rex-code-ergonomics binds.
- [ ] No abbreviations (`cfg`, `ctx` idiomatic OK).
- [ ] Predicates: `is_`/`has_`/`can_`.
- [ ] Verbs on functions, nouns on types.
- [ ] Domain primitives = newtypes (`SourceId`, `InstrumentId`, `Price`, `Qty`, `Notional`,
  `LocalTs`, `VenueTs`, …). Bare `u64` never impersonates another.
  - Replaced 2026-11-13: "(`SourceId`, `InstrumentId`, `Price`, `Qty`, `TsUs`, …)". WHY: one stamp
    type became two clock domains (§5), and quote money got its own type (§4).
- [ ] No `bool` parameters in `pub` functions.
- [ ] IF >3 parameters, THEN use struct.
- [ ] Accept `&str`/`&[T]`/`&Path`. Return owned when allocating.
- [ ] Behavior = methods on type, not free functions in util modules.
- [ ] Derive `Debug, Clone, PartialEq, Eq, Hash` wherever semantics let you.
- [ ] Flat beats nested. Guard clauses / early return / `let-else` / extracted functions over
  branch-in-branch. Deep nesting = review flag. Restructure before merge.
  - Amended 2026-07-18. WHY: deep indent pyramids hide control flow.
- [ ] IF conditional fits one line, THEN write it on one line. The fmt gate enforces this, not
  hand-formatting: `rustfmt.toml` sets `single_line_if_else_max_width` +
  `single_line_let_else_max_width` = 100.
  - Amended 2026-07-18. WHY: trivial branches do not earn vertical space.
- [ ] Statement `if` without else is always multi-line under rustfmt. Prefer expression form /
  `matches!` / combinators where the one-liner reads better.
- [ ] Design APIs from call site backwards. Strategy code = customer.

## 10. Comments & Documentation

- [ ] rex-code-commenting binds.
- [ ] WHY only, never WHAT. Default = no comment.
- [ ] Fix design first (rename, extract, newtype). Comment only what code cannot say.
- [ ] In-body comments only when behavior genuinely cannot be self-evident: vendor quirk, trap,
  measured justification.
- [ ] Every module: `//!` why-header.
- [ ] `pub` item: `///` ONLY when contract exceeds name + signature (invariants, units, panics,
  warnings, non-obvious errors).
- [ ] Self-evident `pub` item = zero docstring. Never restate signature.
- [ ] Forbidden:
  - commented-out code
  - history notes
  - drifting TODOs (ticket or fix)
  - section dividers (extract functions instead)
  - generated boilerplate commentary

### Amended 2026-08-07: no document references in comments

WHY: user directive. A comment that cites a plan, milestone id, audit finding number, or a
numbered section of any document is noise to a reader without that artifact. It rots the moment
the artifact moves.

- [ ] Comments never cite documents by number:
  - no PLAN/CHECKLIST/MASTER references
  - no milestone or ruling ids
  - no `§N`
  - no `finding #N`
  - no vendor-doc section numbers
- [ ] A comment states its WHY in its own words or does not exist.
- [ ] Existing offenders = cleanup debt, not precedent.

### Amended 2026-08-07: comment-cleanup pass

WHY: agents write comments at volume, and a writer cannot proof its own tone.

- [ ] IF agents of a multi-agent run wrote or rewrote comments, THEN the run ends with a dedicated
  comment-cleanup subagent pass. The pass obeys `rex-code-commenting` (2026-11-12 section below).
  The run is not complete before that pass.
  - Replaced 2026-11-12: the pass obeyed "the rex-cleaner-comments charter".
- [ ] The pass enforces this whole section, including the no-document-references rule above.

### Amended 2026-11-12: comment clauses name `rex-code-commenting`

WHY: §1 named a caveman skill for comments, and the cleanup pass named a charter. Neither is in the
skill set of this repo. `rex-code-commenting`, which this section already binds, forbids caveman
style in a comment, so the two clauses could not both hold.

- [ ] `rex-code-commenting` binds each comment: short plain English, WHY only. One exception: a
  comment cites no document by number, not even a ticket or a vendor-doc section that the skill
  permits (2026-08-07 above).
- [ ] The cleanup pass never deletes a warning, a `SMELL:` flag, a hazard note, or a `SAFETY:`
  comment. Each one stays until the commit that removes its problem.
- [ ] An old comment in a style that the skill forbids stays until a task changes its file for its
  own work. That task brings it up to the skill. A move task restyles no comment. A sweep of old
  comments is its own task.

## 11. Dependencies

- [ ] Allowlist = table + crate notes below. Allowlist = constitutional.
- [ ] To add a crate, do a major upgrade, or expand features: amend this doc with one-line
  justification.
- [ ] Always: at adoption, make sure version = latest stable. `default-features = false`. Only
  features that are in use.

| Crate                                                        | Status                                             | Role                             |
| ------------------------------------------------------------ | -------------------------------------------------- | -------------------------------- |
| `tokio`                                                      | ratified. See note.                                | async actor runtime              |
| `rtrb`                                                       | ratified                                           | SPSC rings                       |
| `serde` + `serde_json`                                       | ratified                                           | exchange payload deserialization + edge DTO serialisation |
| `thiserror`                                                  | ratified. See note.                                | library error types              |
| `anyhow`                                                     | ratified (binaries only)                           | binary error context             |
| `parquet` (`arrow`, `zstd`) + `arrow-array` + `arrow-schema` | ratified 2026-07-18. See note.                     | persistence                      |
| `proptest`                                                   | ratified (dev)                                     | fitness suite                    |
| `tokio-tungstenite`                                          | ratified (user directive 2026-07-18). See note.    | WS streams                       |
| `reqwest`                                                    | ratified (user directive 2026-07-18). See note.    | REST snapshots + backfills, OTLP export (feature otlp) |
| `futures-util`                                               | ratified 2026-07-18. See note.                     | WS stream/sink combinators       |
| `serde-saphyr`                                               | ratified 2026-07-18. See note.                     | configuration                    |
| `core_affinity`                                              | ratified 2026-07-18. See note.                     | pinning                          |
| `eframe` (`glow`, `default_fonts`)                           | ratified 2026-07-23, amended 2026-10-26. See note. | browser GUI (wasm32 only)        |
| `ring`                                                       | ratified 2026-07-27. See note.                     | request signing                  |
| `k256`                                                       | ratified 2026-08-07. See note.                     | secp256k1 signing                |
| `tiny-keccak`                                                | ratified 2026-08-07. See note.                     | keccak256                        |
| `rmcp`                                                       | ratified 2026-10-14. See note.                     | MCP server (feature `mcp`)       |
| `rustix`                                                     | ratified 2026-10-14. See note.                     | SIGTERM (feature `mcp`)          |
| `hyper`                                                      | ratified 2026-10-15, amended 2026-10-26. See note. | HTTP server (`mcp`, `web`)       |
| `hyper-util`                                                 | ratified 2026-10-15, amended 2026-10-26. See note. | HTTP server (`mcp`, `web`)       |
| `http-body-util`                                             | ratified 2026-10-26. See note.                     | response bodies (feature `web`)  |
| `web-sys`                                                    | ratified 2026-10-26. See note.                     | browser APIs (wasm32 only)       |
| `js-sys`                                                     | ratified 2026-10-26. See note.                     | JS values (wasm32 only)          |
| `wasm-bindgen-futures`                                       | ratified 2026-10-26. See note.                     | browser task spawn (wasm32 only) |
| `web-time`                                                   | ratified 2026-10-26. See note.                     | wasm32 clock (wasm32 only)       |
| `quanta`                                                     | ratified 2026-11-12. See note.                     | latency ticks (native only)      |
| `raw-cpuid`                                                  | ratified 2026-11-12. See note.                     | invariant TSC check (x86_64)     |
| `hdrhistogram`                                               | ratified 2026-11-12. See note.                     | latency percentiles (native only) |
| `smol_str`                                                   | candidate: only on demonstrated ergonomic need     | edge string handling             |
| `criterion`                                                  | candidate (dev): only if benchmark warranted       | measurement                      |

### Crate notes

Each note = justification + limits of that crate's row. Same force as the row.

- `tokio`
  - Features grow by milestone. Minimal set only. Never umbrella `full`.
  - M09: `rt` + `time`.
  - M11: `net`.
  - M12 adds `rt-multi-thread` + `signal` + `macros`. For: the config-sized multi-worker runtime
    that PROJECT §2 mandates, SIGINT/SIGTERM-driven graceful drain, and `select!` on the shutdown
    wait.
  - Under feature `mcp`, rmcp adds `io-std`. `io-util` and `sync` were already unified in.
    polysim adds no tokio feature itself.
  - Amended 2026-10-15: test builds gain `fs`, through reqwest 0.13's `stream` feature in rmcp's
    client. Non-test builds gain nothing. polysim adds no tokio feature itself.
  - Amended 2026-10-26: features split by target. Every target takes `rt` only. The native table
    adds `macros`, `net`, `rt-multi-thread`, `signal` and `time`. WHY: the lib now builds for
    wasm32 in a browser tab, which has no sockets, signals or worker threads to run them on, so
    only the native build asks for them. `polysim-web` adds no tokio feature: its current-thread
    runtime uses the native set.
- `thiserror`
  - `default-features = false` + `std` only.
  - Amended 2026-10-26. WHY: an error in the lib formats a `PathBuf`, which needs `std`. The
    native build only compiled because another crate had switched the feature on. A feature the
    lib needs is declared by the lib, never left to unification.
- `parquet` (`arrow`, `zstd`) + `arrow-array` + `arrow-schema`
  - M09: arrow-rs 59.1.0.
  - For: ArrowWriter batched append + footer KV + explicit row-group flush + arrow reader for M12
    read-back.
  - `arrow` feature pulls only needed arrow-\* sub-crates, not umbrella.
  - ZSTD = doc-recommended balance codec. Encode is off hot path.
  - MSRV 1.85 < toolchain.
  - Breaking majors ≤ quarterly. Three crates lock-step.
- `tokio-tungstenite`
  - 0.30, `connect` + `rustls-tls-webpki-roots`: ring stack.
- `reqwest`
  - 0.12 PINNED with `rustls-tls`: ring + webpki, ONE TLS stack shared with tungstenite.
  - 0.13 rejected: it bundles aws-lc-rs second crypto provider + C toolchain.
  - Amended 2026-10-15: a second major, 0.13.5, enters TEST builds only, through rmcp's
    streamable-HTTP client. It carries no TLS feature (`json` + `stream` only). Thus no second
    crypto provider and no C toolchain. polysim's own client stays 0.12.
- `futures-util`
  - M11: `StreamExt::next`/`SinkExt::send` on WebSocketStream. tungstenite does not re-export
    them.
  - `sink` + `std` features only.
  - Already transitive in lock.
- `serde-saphyr`
  - YAML config.
  - `serde_yaml` = archived. `serde_yaml_ng`/`serde_norway` forks = stale >12mo.
  - saphyr family alone is actively maintained. It is pure-safe-Rust.
  - It honors `deny_unknown_fields`, tagged enums included, with field-naming errors. Verified.
- `core_affinity`
  - Hot-thread CPU pinning.
  - Maintained + far wider use than `affinity`.
  - Per-OS implementations.
  - `set_for_current` needs no unsafe on our side.
  - macOS: best-effort no-op.
- `eframe` (`glow`, `default_fonts`)
  - Browser workstation GUI. PINNED `=0.35.0`. `default-features = false`.
  - `glow`: the WebGL2 renderer. `default_fonts` = TEMPORARY until repo-owned faces are embedded.
    Then drop it.
  - egui/eframe types are UI-thread only. They never cross the hot-path boundary (§2).
  - No `egui_plot`/docking/table/theme crate. No direct `wgpu` dep.
  - Upgrades deliberate: egui breaks APIs between minors. Re-verify the handbook on any bump.
  - Declared ONLY under `[target.'cfg(target_arch = "wasm32")'.dependencies]`.
    - Amended 2026-10-26. WHY: the workstation is a browser page that Jet hosts, so no native
      build has a use for a renderer. Thus a trading engine, `polysim-mcp` and `polysim-web` never
      build eframe, winit or X11/wayland, and no feature can reach it.
    - The fitness config-guard pins it: eframe sits in no other table, its version stays
      `=0.35.0` with default features off, and no `default` or `ui` feature exists.
  - Hidden-boot quirk: eframe 0.35 drops the texture uploads of a frame that is drawn while the
    page is hidden, and the canvas then stays black for good. Thus the app starts, link included,
    when the page is first shown. Fixed upstream in eframe 0.36, which needs Rust 1.95. This repo
    pins 1.92, so 0.36 waits for a deliberate toolchain bump (§14).
  - Honest lock delta, measured over the whole move to the browser: 541 -> 449 packages. New: cgl,
    egui_glow, glutin, glutin-winit, glutin_egl_sys. Gone: 97 entries: the wgpu/naga/ash stack,
    accesskit with its zbus/atspi/async-* stack, the windows family, and eight duplicate
    versions. Only egui_glow compiles, and only for wasm32. The lock names the other four and no
    target builds them.
  - Replaced 2026-10-26: eframe was the native macOS trading GUI. `wgpu` was the Metal renderer
    and `accesskit` the native accessibility. It was `optional = true` behind feature `ui`.
    - Amended 2026-07-25. WHY then: a headless trading engine deployed in another region must not
      need wgpu/winit/X11/wayland merely to BUILD.
    - `desktop/` was `#[cfg(feature = "ui")]` (amended 2026-10-14: only its painters and window).
      The `polysim-ui` bin and the `dom-fixture` example carried `required-features = ["ui"]`.
- `ring`
  - HMAC-SHA256 signing of Binance private WS-API + REST requests.
  - ALREADY transitive in the lock via rustls' ring stack, reached from both `reqwest` and
    `tokio-tungstenite`. This is the `futures-util` precedent. Thus the direct dep adds no build
    cost.
  - A RustCrypto `hmac` + `sha2` pair would put a SECOND crypto stack in the tree for no gain.
  - Only `ring::hmac` is used.
  - Hex encoding and query-string building are hand-rolled. `hex`/`form_urlencoded` are not
    pulled.
  - `default-features = false` and NO features. `ring::hmac` carries no `alloc` gate. Only
    `ring::rsa` does. Verified against `ring-0.17.14/src/lib.rs:142`, a file of that crate.
- `k256`
  - Polymarket execution plan.
  - secp256k1 recoverable ECDSA for the EIP-712 `ClobAuth` and order signatures. The venue's
    entire auth model rests on those signatures.
  - `ring` exposes NO secp256k1. Thus there is no in-stack alternative.
  - `default-features = false` + `ecdsa` only.
  - Signing is RFC-6979 DETERMINISTIC. Thus no RNG is used, no `getrandom` edge is added, and
    every signature is replayable under §2 and pinnable by vector.
  - Pure Rust. MSRV 1.85 < toolchain.
  - Knowingly a SECOND crypto stack beside `ring`. `ring` keeps L2 HMAC. Accepted because the
    only alternative is `alloy`: an enormous tree for two struct hashes.
  - Honest cost, measured not assumed: `rfc6979` drags RustCrypto `hmac` + `sha2` in
    transitively, and neither was in the lock before. Unavoidable: RFC-6979 is defined in terms of
    HMAC over the curve's digest.
- `tiny-keccak`
  - Polymarket execution plan.
  - keccak256 for EIP-712 struct/domain hashing, EIP-55 address checksums, and the ERC-7739 wrap.
  - Ethereum uses ORIGINAL Keccak padding, NOT SHA3-256. Thus neither `ring` nor `sha2` can
    supply it.
  - `default-features = false` + `keccak` only.
  - Its one dependency, `crunchy`, was already in the lock. Thus it adds no build cost.
- `rmcp`
  - The official MCP SDK. 3.5.1 = latest stable at adoption. MSRV 1.88 < toolchain.
  - Features: `server`, `macros`, `transport-io`. `client` only as a dev-dependency, for the
    fitness suite's in-process client.
  - `optional = true` behind feature `mcp`. Only the `polysim-mcp` bin requires it. Thus a trading
    engine never builds it.
  - Honest cost, measured: 18 new crates in the lock (darling, darling_core, darling_macro,
    dyn-clone, futures, futures-executor, ident_case, pastey, ref-cast, ref-cast-impl, rmcp,
    rmcp-macros, schemars, schemars_derive, serde_derive_internals, strsim, tokio-stream,
    tokio-util).
  - `tracing` is an unconditional rmcp dependency. It was already in the lock via reqwest ->
    hyper-util. The §7 2026-10-14 amendment binds it.
  - A dev-dependency cannot be optional. Thus every test build compiles rmcp's client half, with
    or without `mcp`. A deploy build never builds dev-dependencies.
  - Amended 2026-10-15: features gain `transport-streamable-http-server`, for `--http`. The
    dev-dependency gains `transport-streamable-http-client-reqwest`, the HTTP pin's client. Honest
    cost, measured:
    - New to the lock: base64 0.23.1, httpdate 1.0.3 (hyper's server half) and sse-stream 0.2.6 in
      every `mcp` build. reqwest 0.13.5 in test builds only, with no TLS feature. wasm-streams
      0.5.0, a wasm-only dependency of it, in the lock and never compiled on a native target.
    - New to an `mcp` build (non-test): base64, httpdate, sse-stream, plus async-trait 0.1.91 (a
      proc macro, from rmcp's session layer) and tokio-stream 0.1.19 (already in the lock, reached
      only by test builds before).
    - A dev-dependency cannot be optional. Thus every test build compiles that client.
- `rustix`
  - 1.1.5 = latest stable at adoption. `default-features = false` + `process` only.
  - The only safe-Rust way to send SIGTERM under `forbid(unsafe_code)`. std's `Child::kill` sends
    only SIGKILL, which skips the engine's drain. SIGKILL still goes through std.
  - Honest cost: the lock still names it for eframe's native windowing stack (wayland, x11,
    polling). eframe builds for wasm32 only, so that stack never compiles, and feature `mcp` is the
    one thing that builds rustix, on every target. Thus a macOS build or an mcp-only build compiles
    it for the first time. This is not the `ring` precedent. Amended 2026-10-26: it was reached
    through eframe -> zbus/accesskit_unix, a Linux-only route that left with the native window.
  - `optional = true` behind feature `mcp`.
- `hyper` + `hyper-util`
  - Serve rmcp's streamable-HTTP service on one loopback listener: `hyper::server::conn::http1`
    per connection, hyper-util's `TokioIo` + `TowerToHyperService`.
  - Both were already in the lock via reqwest. This is the `ring` precedent. The direct deps add
    one crate, `httpdate` (hyper's server half).
  - hyper 1.12.0 + hyper-util 0.1.21 = latest stable at adoption. Adopting them moves the shared
    entries reqwest already used: hyper 1.10.1 -> 1.12.0, hyper-util 0.1.20 -> 0.1.21, want 0.3.1
    -> 0.3.2. All are semver-compatible.
  - hyper `server` + `http1` only. hyper-util `tokio` + `service` only.
  - HTTP/2 is out: `server-auto` adds h2 + fnv for no MCP client.
  - axum rejected: a router framework for one route.
  - `optional = true` behind feature `mcp`. Thus a trading engine never builds the server half.
  - Amended 2026-10-26: feature `web` shares both, for `polysim-web`'s loopback server and its
    WebSocket upgrade. No new feature of either crate, and the same `http1`-only rule holds. The
    relay's WebSocket is the ratified `tokio-tungstenite` over the upgraded connection. Thus the
    trading engine still builds neither crate, since the features are `optional = true` and only
    the `polysim-mcp` and `polysim-web` bins ask for them.
- `http-body-util`
  - 0.1.5 = latest stable at adoption. `default-features = false`, NO features.
  - `Full` is the response body of `polysim-web`.
  - Already in the lock via reqwest 0.12 (also rmcp and sse-stream). This is the `ring`
    precedent: the direct dep adds no crate. Adopting it moved the shared entry 0.1.4 -> 0.1.5,
    semver-compatible.
  - `optional = true` behind feature `web`. Thus a trading engine never builds it.
- `web-sys`
  - 0.3.106 = latest stable at adoption. `default-features = false` + the feature gates the
    workstation names: `AddEventListenerOptions`, `BinaryType`, `Document`, `HtmlCanvasElement`,
    `Location`, `MessageEvent`, `Url`, `WebSocket`, `Window`, `console`.
  - For: the relay WebSocket, the document and canvas, the page title, the console log sink.
  - wasm32 table only. Already in the lock via eframe. This is the `ring` precedent: no new
    crate, only the feature gates above.
  - No `#[wasm_bindgen]` attribute in polysim: its expansion contains `unsafe`, and the crate is
    `forbid(unsafe_code)` (§14). The `wasm_bindgen` types come re-exported through `web_sys`.
  - web-sys, js-sys and wasm-bindgen-futures are one family: they move together, with the
    `wasm-bindgen` crates under them (0.2.126 -> 0.2.129, semver-compatible, and the macro crate
    now builds on the `syn` 3 that the lock already held). The `wasm-bindgen` command-line tool
    that Trunk fetches must match the lock's version, and Trunk reads it there.
- `js-sys`
  - 0.3.106 = latest stable at adoption. `default-features = false`, NO features.
  - For: `ArrayBuffer` and `Uint8Array` on the relay socket, and `Math::random` for the
    workstation's sender nonce.
  - wasm32 table only. Already in the lock via eframe. Moves with web-sys.
- `wasm-bindgen-futures`
  - 0.4.79 = latest stable at adoption. `default-features = false`, NO features.
  - For: `spawn_local`, which runs eframe's async start on the page.
  - wasm32 table only. Already in the lock via eframe. Moves with web-sys.
  - Honest cost: its lock entry names `tokio`, behind a cfg (emscripten with an unstable flag)
    that no polysim target meets. No tokio code is built through it.
- `web-time`
  - 1.1.0 = latest stable at adoption. `default-features = false`, NO features.
  - For: `Instant`, `SystemTime` and `UNIX_EPOCH` on wasm32, where std's clock panics. The lib's
    `msg::time` module switches the import by target, so no caller notices.
  - wasm32 table only. Already in the lock via eframe.
- `quanta`
  - WHY (2026-11-12): the hot thread stamps latency with raw counter reads of a few ns, with no
    `unsafe` in polysim. `Instant::now()` costs 15 to over 50 ns and reads µs only.
  - 0.13.0 = latest stable at adoption. MSRV 1.87 < toolchain. `default-features = false`, NO
    features: its defaults `flaky_tests` and `mock` stay off in each build that ships.
  - Native table only: `[target.'cfg(not(target_arch = "wasm32"))'.dependencies]`. The browser
    build takes no latency stamps, so it has no `quanta` and no counting `Stamper`.
  - For: `Tick` and `Stamper` of `msg::time`. The driver reads 2 ticks for each message, the command
    sink 1 for each banked command, and a sender 2 for each command it sends. The metrics thread
    turns ticks into time through the same `quanta::Clock`.
  - Dev feature `mock`: one line in `[target.'cfg(not(target_arch = "wasm32"))'.dev-dependencies]`,
    so that a test scripts each tick through `Clock::mock()`. Cargo adds a dev feature only to
    builds of tests and examples. Thus no engine binary builds `mock`.
  - A tick is never engine state (§2). `Tick` has no ordering, no arithmetic and no conversion on
    the hot side. Only the metrics thread converts it.
  - A counter that is not invariant does not refuse the start: `quanta` then reads the OS clock and
    gives no signal, so the start check names the clock source.
  - Honest cost, from its manifest at adoption: new crates `quanta`, `portable-atomic` (feature
    `fallback`), `raw-cpuid` (x86 and x86_64 only), and a lock entry `wasip1` that no polysim
    target builds. `libc`, `windows-sys` and `wasip2` were in the lock already. The lock diff of the
    commit that adds the line is the measured form.
- `raw-cpuid`
  - WHY (2026-11-12): on x86_64 the start check must know whether the TSC is invariant, because
    `quanta` falls back to the OS clock with no signal.
  - 11.6.0 = latest stable at adoption. It declares no MSRV, edition 2018. `default-features =
    false`, NO features: it has no default feature.
  - x86_64 table only: `[target.'cfg(target_arch = "x86_64")'.dependencies]`. An aarch64 host and
    the linux/arm64 deploy image never build it.
  - For: one read of the invariant-TSC flag at start, nothing else. The CPUID instruction is
    `unsafe` inside the crate, never in polysim.
  - `quanta` pulls the same crate on x86 and x86_64. Thus the direct line adds no crate. This is the
    `ring` precedent. Its one dependency, `bitflags` 2, was in the lock already.
- `hdrhistogram`
  - WHY (2026-11-12): percentiles with a bounded error, from memory of a fixed size, on the metrics
    thread. The power-of-two lanes that they replace err by up to 2 times and show a mean.
  - 7.6.0 = latest stable at adoption. MSRV 1.88 < toolchain. `default-features = false`, NO
    features: `serialization` (flate2, nom, base64) and `sync` (crossbeam-channel) stay off.
  - Native table only: its one user, the metrics thread below `runtime/assembly/`, compiles for
    native targets only.
  - For: p50, p99, p99.9 and max of each latency stage, for each interval and for the rolling
    window, with `u32` counts. Each histogram is allocated at start.
  - Never on the hot thread. The hot thread pushes raw samples, and a histogram update there is a
    defect.
  - Honest cost, from its manifest at adoption: new crates `hdrhistogram` and `byteorder` 1.5.0
    (MSRV 1.60). `num-traits` was in the lock already. The lock diff of the commit that adds the
    line is the measured form.

### Amended 2026-10-26: the egui-free half of the workstation compiles in every build

WHY: the model, view, text and link-session files name no egui type. Gating them behind the
renderer would make a native build, and the fitness suite with it, stop checking them.

- [ ] Only files that name an egui/eframe/web-sys type live in `operator/workstation/browser/`. That
  folder compiles only for wasm32: the painters, the app, the relay socket and the boot path.
- [ ] Everything else under `operator/workstation/` compiles for every target and is
  fitness-tested natively: the link session, attachment, models, views and `text/`.
- [ ] The words and colour roles the page shows live in the toolkit-free
  `operator/workstation/text/`, and the painters read them there. Thus a native test can pin what
  the page says.
- [ ] Each target lints the half it runs. The wasm32 clippy command (§14) is what lints
  `operator/workstation/browser/`, and it is the target that warns on dead workstation code.
- [ ] Rule intact: a native build links no eframe, wgpu or winit.
- Replaced 2026-10-26: Amended 2026-10-14, the same rule for `desktop/` with feature `ui`. WHY then:
  the workstation bridge rendered the same model headless, so the egui-free files could not wait
  on the GUI stack. Only files naming an egui/eframe type were `#[cfg(feature = "ui")]`: the
  painters, the window and `run_desktop`. The words and colour roles lived in `desktop/text/`, so
  the window and the bridge could not say different things.

### Amended 2026-10-14: serde serialises edge DTOs

WHY: edges write JSON through `serde_json`. A hand-written JSON writer would be a second
serializer with its own escaping bugs, for a crate already in the tree.

- [ ] The `serde` + `serde_json` role widens from exchange payload deserialization to edge DTO
  serialisation: venue request bodies, the exposure file, the relay hello and the app descriptor
  that `polysim-web` writes. Amended 2026-10-26: these last two replace the bridge protocol lines.
- [ ] No new crate and no new feature: `derive` + `std` already cover it.
- [ ] DTOs stay at the edge. A hot-path type never derives `Serialize`.
- [ ] Amended 2026-11-13: the OTLP bodies of the export are edge DTOs too.

### Amended 2026-11-13: the OTLP export adds no crate

WHY: OTLP over HTTP with JSON bodies needs an HTTP client, a JSON writer and a runtime, and the
engine links all three.

- [ ] Feature `otlp` of package `engine` is `otlp = []`. It enables no crate and no feature of a
  crate, so `cargo tree -p engine -e normal` prints the same crates with and without it.
- [ ] `reqwest` 0.12: the exporter posts with it, http only, to a collector on loopback. Its client
  has no proxy, keeps no idle connection, retries nothing, and follows no redirect, so each request
  goes out at most one time, to the collector only. The body is set by hand: no `json` feature.
- [ ] `tokio`: the exporter runs a current-thread runtime on its own thread, with `rt`, `net`,
  `time` and `macros` of the native table. polysim adds no tokio feature.
- [ ] `serde` + `serde_json` write the OTLP bodies. They are edge DTOs (2026-10-14 section above).
  No hot-path type derives `Serialize`.
- [ ] Not adopted, measured on 2026-11-13 against the lock of `ec891c2`. Each set had no HIGH or
  CRITICAL finding of `trivy`:
  - `opentelemetry-proto` 0.33.1 with serde: 7 new lock entries and 10 crates new to the engine
    build, for types that the export writes in about 200 lines.
  - `opentelemetry` + `opentelemetry_sdk` + `opentelemetry-otlp` 0.33.1 over HTTP: 9 new lock
    entries and 20 crates new to the engine build, `reqwest` 0.13 among them. The SDK starts a batch
    thread for logs and a reader thread for metrics. Its HTTP exporter sends a request again after
    a timeout by default.
  - Any gRPC set (`opentelemetry-otlp` with `grpc-tonic`): 13 new lock entries and 26 crates new to
    the engine build, with an HTTP/2 stack (`h2`, `tonic`).

## 12. Testing

- [ ] Two tiers only: fitness suite + live-network tests. Lanes and real funds: last section of
  §12.
  - Amended 2026-07-18: unit tier removed. WHY: test mass concentrates in fitness + human-run
    integration.
- [ ] Fitness suite: small, immortal. `engine/tests/fitness/`, proptest + recorded fixtures. The
  only CI tests. Content:
  - FastQueue never loses/reorders data
  - order-book snapshot+deltas ≡ reconstructed state
  - ingress ordering rules
  - zero steady-state alloc
  - adapter parse/sequencing vs recorded WS + REST payloads
  - link wire format (amended 2026-07-25)
  - quant leaf-math pins (amended 2026-07-28)
  - web server guard + relay (amended 2026-10-26)
  - Amended 2026-11-12: a fitness test can be replaced. Date, WHY, and the rule: the 2026-11-12
    section on replaced fitness tests below.
- [ ] Live-network tests: `engine/tests/integration/`, gated `#[ignore]`. Never CI. Only the
  `live-runner` role of a job runs a signed test or the probe, by a runbook and the live policy. An
  agent runs a public read only where a plan names it, by its exact name and with an empty
  environment.
  - Date, WHY, and the rest of this rule: the 2026-11-12 section on lanes and real funds, last
    section of §12.
  - Amended 2026-11-13: the table of what an agent runs against a venue. Date, WHY, and the
    table: the 2026-11-13 section of that name, after the section on seams that change.
  - Amended 2026-11-15: only the role that the plan names runs a public read. It starts the binary
    of target `integration` by its path, with the exact name of the test. The run starts in an
    empty folder outside each checkout. Its environment is empty except `PATH`, `HOME`, and
    `TMPDIR`. WHY: no agent starts an ignored test in a checkout (`AGENTS.md`). `cargo test` starts
    each test in the folder of its package. A public read can write below the temporary folder, so
    the environment keeps `TMPDIR`.
  - Replaced 2026-11-12: the agent ran each live-network test deliberately.
    - Amended 2026-07-18. WHY: user directive. Agent runs ALL tests, live-network included. Network
      reach to venue endpoints is verified. Human is not required.
- [ ] Contract seam tests only when specific seam proves unstable.
  - Amended 2026-11-12: each seam that a change reshapes gets a contract test. Date, WHY, and the
    rule: the 2026-11-12 section on seams that change below.
- [ ] IF bug is found, THEN write failing fitness regression first. Then fix.
- [ ] No mocks unless forced. Preferred fixture = recorded real exchange payloads.
- [ ] §2 determinism makes replay fixtures the backbone of testing.

### Amended 2026-07-20: inline quant unit tests (WITHDRAWN 2026-07-28)

Not in force. Record only.

- WHY then: pure quant calculators can carry inline unit tests.
- Text then: the amendment let `hot/quant` numeric modules pin leaf math with inline
  `#[cfg(test)]` tests. Those tests were kept to the FEW highest-value pins (core correctness +
  found-bug regressions), so the fitness charter stays architectural. It was not a general
  reopening of the unit tier: it was scoped to pure calculators, and sparseness was deliberate.

### Amended 2026-07-25: link wire format in fitness

WHY: the link hand-rolls its wire format across a process boundary. Thus encode/decode agreement
is an architectural invariant. Socket behavior is not, and the charter stays architectural.

- [ ] Fitness gains the link wire format:
  - encode/decode round-trip proptest per frame kind
  - the `(sender, boot, topic)` seq-gate logic (dedup, reorder, restart resets the gate)
- [ ] Socket-level behavior stays OUT: subscription TTL expiry, subscribe refresh, the
  peer-restart gate over a real socket. It is contract-seam/integration territory.
- [ ] Amended 2026-11-18: the golden file of the frames,
  `engine/tests/fitness/link/link_frames.txt`, starts with the line `layout 0x<digest>` of
  `LINK_LAYOUT`. A change of bytes under an equal digest is red, in bless mode too, and bless mode
  rewrites the file only for a new digest (§6, 2026-11-18 section on the layout of a frame). WHY: a
  golden file that bless mode rewrote with no check pinned nothing.

### Amended 2026-07-28: tests live only under `<package>/tests/`

WHY: user directive. `<package>/src/` carries production code only, and a reader who looks for a
test looks in ONE place.

- [ ] The 2026-07-20 carve-out is WITHDRAWN.
- [ ] No `#[cfg(test)]` block lives anywhere outside `<package>/tests/`: not in `<package>/src/`,
  not in `strategies/`, not in `tools/`.
- [ ] Quant leaf-math pins live in the fitness target under `engine/tests/fitness/hot/quant/`. One
  module per calculator, named for the concept, not the source path.
- [ ] The cost is deliberate and binding: a test under `<package>/tests/` sees only the crate's
  PUBLIC API. Thus re-express a pin on a private item against a public seam, or drop it.
- [ ] Forbidden: `pub(crate) -> pub` to make a test compile. WHY (§8): `pub` is a promise, and a
  test is not a caller worth promising to.
- [ ] An item can still be promoted on its OWN merits by explicit ruling.
- [ ] A test that follows a merit-based promotion is a consequence of it, never the argument for
  it.
- Two items were promoted on the day this passed:
  - `runtime::ExecutionLease`: exclusive live-execution ownership is an engine-level promise that
    its already-exported peers imply.
    - Amended 2026-11-18, as built: `ExecutionLease` now holds only the run nonce of its venue. The
      ownership of an identity is `IdentityLock`, taken in each mode (§2, 2026-11-13 section on
      preflight). The lease keeps its path and its promotion. WHY: the lock left the lease.
  - `hot::quant::optimise`: every sibling calculator module was `pub` already. It was the lone
    exception.

### Amended 2026-10-14: the bridge wire format is a cross-process pin (WITHDRAWN 2026-10-26)

Not in force. Record only.

- WHY then: Jet's page read the bridge's JSON lines from another process and another repository. A
  renamed or reshaped field broke the page silently, which is the same failure the link wire
  format amendment guards.
- Text then: fitness gained the bridge wire format: a fixed scene encoded to a checked-in
  snapshot, and the encoder's change and append rules were pinned through its public API. The
  process-level check (spawn the bin, read stdout to `bye`) opened a socket, so it lived in
  `tests/integration/`, `#[ignore]`d like the rest of that suite. The bridge's encoder and command
  types were `pub` on merit, because the wire they produced and read was a promise to another
  program.
- WHY withdrawn: the bridge, its wire and its page are gone (§8, 2026-10-26).

### Amended 2026-10-26: the web server is pinned in fitness, through real sockets

WHY: the guard is the only thing between a web page and unauthenticated run control. A guard that
a refactor could loosen with every gate green is a remote-start primitive for any site the
operator visits.

- [ ] `engine/tests/fitness/operator/web_http.rs` is in fitness. It spawns `polysim-web` as its own
  process, with a temporary bundle and a fake engine on a loopback UDP socket, and pins:
  - the descriptor bytes and content type, with and without `--mcp`
  - the bundle served from memory, with each file's own type, and nothing outside it
  - a missing bundle stops the process with the Trunk hint
  - the guard: a foreign `Origin`, `Origin: null`, a non-loopback `Host`, and a missing or
    repeated `Host` are refused before any route, and an unknown peer is a 404
  - the relay: the hello comes first, one datagram crosses each way byte for byte, a frame over
    the datagram limit closes the socket and reaches no engine, and a down engine does not end
    the relay
- [ ] This OVERRIDES the 2026-07-25 text that keeps socket-level behaviour out of fitness, as the
  MCP server's `engine/tests/fitness/operator/fleet/mcp_http.rs` already does in practice. The
  charter stays architectural: the pinned thing is an exposure boundary, not socket plumbing.
- [ ] The test drives real sockets with hand-written requests. A client library would tidy away
  the very headers under test.
- [ ] Amended 2026-11-18, as built: the pins of the guard and of the relay moved to
  `engine/tests/fitness/operator/relay.rs`: `web_guard_runs_before_each_route` (the guard, each
  route) and `tab_subscribe_is_admitted_by_the_real_link_guard` (the relay into the real guard of
  the engine). `web_http.rs` keeps the descriptor, the bundle, the missing bundle, and the unknown
  peer. `engine/tests/fitness/operator/accept.rs` pins the accept loop
  (`accept_errors_past_cap_give_the_class_of_the_caller`), and
  `engine/tests/fitness/operator/fleet/fleet_contract.rs` and `fleet_server.rs` pin the fleet
  server. `mcp_http.rs` keeps four tests of the transport. WHY: each test of a box of part 5
  replaced the tests that pinned its old form (2026-11-12 section on replaced fitness tests).

### Amended 2026-11-13: the OTLP export is pinned in fitness through a loopback collector

WHY: what leaves the process, and the Never line on a timeout, are boundaries that a refactor can
break with each gate green.

- [ ] `engine/tests/fitness/runtime/otlp.rs` is in fitness, as a contract test. A stand-in collector
  listens on loopback in the test process. It answers, refuses, redirects, or never answers.
- [ ] It checks each body against the official OTLP examples of `opentelemetry-proto`, kept byte for
  byte in `engine/fixtures/otlp/` with the license of that repository beside them. So the test does
  not only agree with the encoder that it tests.
- [ ] This extends the 2026-10-26 override of the 2026-07-25 text to the export. The pinned thing is
  what leaves the process and when the exporter gives up, not socket plumbing.

### Amended 2026-11-12: lanes and real funds

WHY: a signed test or the probe uses the live keys of a real account. Each agent reads this law
and the ignore reason of each test in each test run. A validation plan names three lanes, and this
law names two tiers, so each lane needs one place.

- [ ] Lane `fitness` and lane `contract` live in test target `fitness` of a lib crate:
  `engine/tests/fitness/`. A seam test is a module of that target. No folder
  `<package>/tests/contract/`.
- [ ] Each lib crate has at most one fitness target, `<package>/tests/fitness/`. WHY: the counting
  allocator and the suite-integrity test need one test binary.
- [ ] Lane `integration` lives in `engine/tests/integration/`. Each test there is `#[ignore]`d. No
  step of gate G runs it (§14).
- [ ] No unit tier at merge. IF a task writes an inline test scaffold, THEN the same task deletes
  it, or moves it into the fitness target, before its evidence.
- [ ] Who runs a live-network test: the live-network clause at the top of §12. A live run = a
  process that sends a signed request to a venue. Only `live-runner` starts one.
- [ ] A test that signs keeps the code form of `rex-code-tests-integration-testing`, file
  `real-funds.md`: `#[ignore]`, a check of `REX_REAL_FUNDS` first in its body, a dry-run form, a
  kill switch, a rollback, and a runbook section. The runbook section is the procedure of
  `live-runner`. The protocol of that file with a person does not apply: the live policy
  replaces it.
- [ ] IF a test that signs lacks a part of that code form, THEN `live-runner` does not run it.
- [ ] CAUTION: The kill switch of a test cancels only the orders of that test. It never cancels
  each order of the account, because an engine never touches an order that it did not place.
- [ ] An ignore reason, a comment, or a README never names a command that runs an ignored test or
  the probe.

### Amended 2026-11-12: a fitness test can be replaced

WHY: the owner decided that each mechanism of polysim gets a better form, and a test that pins the
old form cannot stay beside it. The knowledge of the suite stays immortal, and a test function can
go.

- [ ] A fitness test goes only after a new test in target `fitness` pins the same behavior with the
  same values, and that new test is green.
- [ ] The unit of deletion is the test function. A module file goes when it holds no test and no
  module that stays uses it.
- [ ] No task cuts inside a test function. IF a test that goes also holds a claim about code that no
  task replaces, THEN that claim first becomes a new test, with the same values, in a module that
  stays.
- [ ] A test whose subject is code that no task replaces stays, also when it runs through a
  replaced mechanism to reach its subject.
- [ ] An old test that stays changes only where a changed interface forces it: the form of a call,
  never an asserted value.
- [ ] Ruling, 2026-11-16: one named exception to the line above. In
  `venues::binance::exec_codec::every_code_maps_to_the_class_the_engine_may_act_on`, the rows of
  -1006 and -1007 on a `Cancellation` assert `RejectClass::Ambiguous`, where they asserted
  `RejectClass::StillLive`. The comment of the test gives the new reason. Ruling D18 of part 4
  made the exception. The new class has its own pins:
  `an_unknown_outcome_of_a_cancel_over_the_websocket_reads_before_any_second_cancel` and
  `an_unknown_outcome_of_a_rest_cancel_reads_before_any_second_cancel`.
  - WHY: the change of class is a behavior change on purpose (the execution contract of part 4,
    end of §8). The test holds the class of each code of the venue in one table. A new test of
    the whole table, and then the removal of the old test, would move each row for two rows.
  - This ruling permits no other changed value. A further exception needs its own ruling here:
    dated, with the test, the rows, and the WHY.
- [ ] Ruling, 2026-11-16: one replaced test with a claim that has no successor. In module
  `venues::polymarket::poly_exec_codec`, the test
  `requests_encode_by_token_and_the_token_table_rebinds_correctly` went whole.
  `requests_encode_by_token_and_the_token_table_rebinds` holds four of its five blocks, each
  value unchanged. Its fourth block held the encoding of `DELETE /cancel-market-orders` for a
  token, and that block has no successor.
  - WHY: correction round 1 of part 4 removed that request on purpose, because it cancels each
    order of the account on the token (third Never line). A claim about code that a task removes
    on purpose has nothing left to pin. The new test says in its doc that no request cancels
    each order on a token.
  - This ruling covers that one block. A further claim with no successor needs its own ruling
    here: dated, with the test, the block, and the WHY.
- [ ] The task that deletes a test records it in the hand-off file of its job: one table row with
  the old test, the new test that pins its behavior, and the module. The hand-off file also holds
  the list of tests (`cargo test -- --list`) before and after.
- [ ] The suite-integrity test of `engine/tests/fitness/scans/` holds during each deletion: a file
  with no `mod` line fails it, and a file that goes with its `mod` line passes.

### Amended 2026-11-12: a seam that changes gets a contract test

WHY: the link token hash is derived in two places, the relay and the engine guard, and no test
drives both. A seam that a change reshapes and that no test drives can break with each gate green.

- Amended 2026-11-18, as built: the token hash is derived in one place, `LinkCredential`
  (`engine/src/link/credential.rs`), and `tab_subscribe_is_admitted_by_the_real_link_guard`
  drives the relay into the real guard of the engine. The WHY above keeps its text as the record
  of the cause. The workstation still derives the strategy hash a second time
  (`engine/src/operator/workstation/link/session.rs`).

- [ ] Each seam that a change reshapes gets a contract test in the same change. The test is a
  module of target `fitness`, in the folder of the top module of the seam (lanes and real funds,
  above).
- [ ] A contract test drives real code on both sides of the seam. Only the outside world is a
  stand-in: the network, the clock, the file system. Time and ticks are passed in.
- [ ] A contract test reaches its seam through the public API (2026-07-28). An item is `pub` for it
  only on its own merit: a caller that is not a test (another compile root, a strategy crate that
  expands a macro, the timing harness), or a module that is a concept in its own right (root
  comment of `engine/src/lib.rs`).
- [ ] Ruling, 2026-11-12: one seam has no such caller, and it is `pub` by this ruling. It is the
  value form of the log drain in `engine::log`: the lanes that a caller builds and hands in, the
  drain loop, its output writer, its deadline and clock passed in, and the counts of written,
  dropped and lost lines and of write errors that a drain returns. WHY: the loss and RESET policy
  of the log (drop counts, reopen of the file, fall back to stderr, deadline) is a seam that
  changes, and `log::init` runs once in a process, so no test can drive it more than one time.
- [ ] This ruling makes no other item `pub`. A further item needs its own ruling in this section.
  The ban of 2026-07-28 on `pub(crate) -> pub` for a test holds for each other item.
- [ ] Ruling, 2026-11-13: two more seams have no caller outside the tests, and they are `pub` by
  this ruling.
  - The drain watchdog of the engine: its deadline and the forced-exit function passed in, and the
    one call that it makes when the drain passes the deadline. Test:
    `drain_past_deadline_calls_the_forced_exit_once`.
  - The exit outcome of the session chassis of a venue edge (`engine/src/venues/edge/mod.rs`,
    `engine/src/venues/exec/exit.rs`): what a session reports when its exit sweep ends, with the
    client ids of the orders that stay, and the decision that gives it from the exit plan, the
    time, and the mirror. Test: `sweep_past_deadline_is_not_swept`.
  - WHY: each is a seam that changes. In a run the watchdog ends the process, so no test can drive
    it with the real forced exit. The chassis runs only inside a live venue edge, and a test cannot
    start one with no key.
- [ ] Ruling, 2026-11-13: the start seam has no caller outside the tests, and it is `pub` by this
  ruling. It is `Preflight` with `run` and `run_with`, `StartPlan`, `StartError`, `Engine` and
  `Engine::start` with its pin parameter (`engine::runtime`), and the venue port trait `VenuePort`
  with its values `VenueScales` and `ProbedVenue` (`engine::venues::port`). Tests:
  `each_refusal_starts_no_thread_and_spends_no_nonce`,
  `pin_failure_refuses_start_before_any_producer`.
  - WHY: a refused start can be driven only with a stand-in venue port and a stand-in pin. The real
    start path, `run_trading_engine`, reads the arguments and a config path, and it is the path by
    which an engine loads `.env`, so no test may drive it.
  - The pin call keeps its own merit (§8, 2026-11-13 section on the fold, start, and exit seams):
    the timing harness passes `pin_to_core`, of type `PinCall`, which returns `PinRefused`.
  - Amended 2026-11-18, as built: the ruling also makes `pub` the identity locks that `Preflight`
    takes: the field `Preflight.locks`, `IdentityLocks` with `of_host`, `in_folder`, `path_of`,
    `take`, `is_held`, and `Default`, and `IdentityLock`, which `take` returns
    (`engine::runtime`). `run_trading_engine` passes `IdentityLocks::of_host()`. A test passes
    `IdentityLocks::in_folder` of its own, so it never meets the lock of a real engine of the host.
    Test: the mode-`off` case of `each_refusal_starts_no_thread_and_spends_no_nonce`. WHY: the
    start seam takes the lock in each mode now, and its field must name a `pub` type.
- [ ] Each other item of 2026-11-13 that a contract test reaches has a caller outside the tests,
  and §8 names it (2026-11-13 section on the fold, start, and exit seams).
- [ ] This ruling makes no other item `pub`. The ban of 2026-07-28 on `pub(crate) -> pub` for a
  test holds for each other item.
- [ ] Ruling, 2026-11-13: the fault seam of the chassis has no caller outside the tests, and it is
  `pub` by this ruling. It is the decision that the session chassis of a venue edge takes on a
  session error: `engine::venues::exec::halt_on_session_error(&ExecSessionError, &FatalSignal)`.
  It reads `failure_class()`, writes one ERROR line with the values, and trips the latch with the
  typed reason. Test: `session_errors_halt_the_engine_with_their_reason`.
  - WHY: the seam changes. Each execution edge tripped the fatal latch at the place of each fault.
    Now one reader of the class at the root of the edge trips it (§6, 2026-11-13). The chassis runs
    only inside a live venue edge, and a test cannot start one with no key.
  - The ruling makes only that function `pub`, and it returns nothing. `EdgeControl::fail` calls
    it, and `EdgeControl` stays `pub(crate)`.
  - Replaced 2026-11-13: "One function takes the `ExecSessionError` and the fatal latch." and "The
    ruling makes `pub` that function and the value that it returns, IF it returns one. The chassis
    calls it, and `EdgeControl` stays `pub(crate)`. §8 names the function as built." WHY: the
    function has its name as built.
- [ ] Ruling, 2026-11-13: the loopback constructor of the signed REST client of Binance has no
  caller outside the tests, and it is `pub` by this ruling. It is
  `SignedRestClient::on_loopback(port, config, quiet)` (`engine::venues::binance::rest`). It takes
  only a port. Each request goes to `http://127.0.0.1:<port>`, signed with a key that is no key.
  Thus it reaches no other host. The engine never builds one, and production keeps
  `SignedRestClient::new`. Test: `a_timed_out_order_action_is_sent_once`.
  - WHY: the seam holds the Never line "send a request again after a timeout". It changed in part
    3: the signed client no longer sends an order action again after a lost answer. The live run of
    the job uses that client, and a test reaches the network only through a stand-in.
- [ ] Ruling, 2026-11-13: the value form of the exposure writer seam has no caller outside the
  tests, and it is `pub` by this ruling. It is `ExposureWriterSetup`, `WriterStatus`,
  `ExposureWriter::spawn`, and `ExposureWriter::lane` (`engine::runtime::exposure`), and
  `ExposureReceiver` with `pop_into` and `ExposureSink::lane` (`engine::hot::sink`). Test:
  `exposure_write_failure_halts_at_limit`.
  - WHY: the halt at a failed exposure write is a seam that changed in part 3. The writer runs on
    its own thread against the file system. Thus a test drives it only with a stand-in writer, as
    with the value form of the log drain (2026-11-12 ruling).
  - `ExposureSink::lane` also has a caller that is not a test: the timing harness.
- [ ] This ruling makes no other item `pub`. The ban of 2026-07-28 on `pub(crate) -> pub` for a
  test holds for each other item.
- [ ] Ruling, 2026-11-15: the fault seam of the market-data feeds has no caller outside the tests,
  and it is `pub` by this ruling. It is `MarketDataError` with its variants `Unreadable` and
  `EpochExhausted`, its methods `failure_class` and `fatal_reason`, and `of_decimal_fault`.
  `of_decimal_fault` gives `Unreadable` for a decimal that the mantissa cannot hold, and nothing for
  another fault. The seam also holds
  `halt_on_market_data_error(feed: &str, &MarketDataError, &FatalSignal)`
  (`engine::venues::edge::fault`). It reads `failure_class()`, writes one ERROR line with the
  feed and the values, and trips the latch with the typed reason. Tests:
  `market_data_faults_halt_the_engine_with_their_reason`,
  `an_overflowing_decimal_ends_the_feed_with_a_market_data_fault`.
  - WHY: the seam changes. The market-data actors tripped the fatal latch at three sites, each at
    the place of its fault. Now one reader of the class trips it: `run_lane`, the root of both
    feeds (§6, 2026-11-13). The actors run only on the socket of a live venue, so a test cannot
    start one.
  - `run_lane` and `SessionEnd` stay `pub(crate)`.
  - Amended 2026-11-16, as built: the first parameter of the function names the feed in the
    ERROR line, and the typed reason does not change. WHY: two feeds share the one root, so the
    line must say which feed stopped.
  - Replaced 2026-11-16: "`halt_on_market_data_error(&MarketDataError, &FatalSignal)`". WHY: the
    function has its feed parameter as built.
- [ ] Ruling, 2026-11-15: the fault seam of the simulator edge has no caller outside the tests, and
  it is `pub` by this ruling. It is `halt_on_sim_error(&SimEdgeError, &FatalSignal)`
  (`engine::venues::sim`). It reads `failure_class()`, writes one ERROR line with the values, and
  trips the latch with the typed reason. Tests:
  `full_simulator_inbox_halts_the_run_and_evicts_nothing`,
  `a_full_command_lane_halts_the_simulated_run`.
  - WHY: the seam changes. The actor of the simulator tripped the latch itself at a full command
    lane, and a full inbox panicked. Now one reader of the class trips it: `SimActor::run`, the
    root of the actor. The actor runs only inside a start of the engine. So a test drives the seam
    with each value of `SimEdgeError`.
  - `SimEdgeError` is `pub` by §8 (2026-11-15 section on the interface of the simulated venue).
    `SimActor` stays crate-private.
- [ ] Ruling, 2026-11-15: the request lanes of the Polymarket edge and the loopback constructor of
  its CLOB client have no caller outside the tests, and they are `pub` by this ruling.
  - The module `engine::venues::polymarket::exec::lanes`: `RestLanes<P>` with `submit`,
    `spawn_lanes`, `LaneSetup`, `Lane`, `RestJob<P>` with `signed` and `public`, `Submitted`, and
    `RestOutcome<P>`. `RestLanes::abort`, `LANE_CAPACITY`, `RestPurpose`, and `OpenOrdersRead` stay
    crate-private.
  - `ClobHttp::on_loopback(port, connect, request)` (`engine::venues::polymarket::exec::rest`). It
    takes no host: each request goes to `http://127.0.0.1:<port>`. Thus it reaches no other host.
    The engine never builds one, and production keeps `ClobHttp::new`.
  - Tests: `cancel_and_heartbeat_finish_while_a_place_blocks`,
    `lost_heartbeat_past_deadline_resets_order_mirror`.
  - WHY: the seam changes. The lanes leave the private actor and become generic over the purpose of
    a job. Three lanes keep a cancel and a heartbeat from waiting behind a held placement. The lanes
    run only inside the actor of a live edge. A test reaches the network only through a stand-in,
    as with `SignedRestClient::on_loopback` (2026-11-13).
  - Amended 2026-11-16, as built: the ruling also makes `pub` `Submitted::is_queued`, the fields
    `purpose` and `request` of `RestJob<P>`, the fields `purpose` and `answer` of
    `RestOutcome<P>`, and the fields of `LaneSetup`. The client takes its base address from a field:
    the host of the venue in production, `http://127.0.0.1:<port>` from `on_loopback`. WHY: a test
    reads the outcome of each job, and builds the lanes from their setup.
- [ ] Ruling, 2026-11-15: the pure gate of the Polymarket start has no caller outside the tests,
  and it is `pub` by this ruling. It is `assess`, `ProbeFacts`, `WalletConfig`, `AdmittedWallet`
  with `assess_account`, and `AccountStanding` (`engine::venues::polymarket::exec::assess`).
  - `assess` takes the facts of the probe, the wallet config, and the local time. It refuses a
    wallet type that is not proven and a protocol version that is not 2. It also refuses a local
    clock that is not valid. It reports the geoblock flag. `preflight_polymarket` calls it before
    its first signed request.
  - `assess_account` refuses a closed-only account, after the signed reads. It takes an
    `AccountStanding` (`Open` or `ClosedOnly`), not a `bool` (§9). `AccountStanding` is `pub` only
    because `assess_account` takes it.
  - `PolymarketPreflightError` and `preflight_polymarket` are `pub` already. The error gains
    `LocalClockInvalid { seen }`, and `preflight_polymarket` takes the secrets from its caller.
  - Tests: `preflight_gates_run_in_order_over_recorded_answers`,
    `invalid_local_clock_refuses_the_arm`. `each_order_vector_equals_sdk_body` builds its signer
    through `assess`. The signed test `venue_accepts_one_signed_order` runs `assess` over the facts
    of the live venue.
  - WHY: the gate is a seam that changes. A local clock that is not valid now refuses the arm before
    any signed request. The real start path loads `.env`, so a test must not drive it.
  - Amended 2026-11-16, as built: the ruling also makes `pub` the fields of `ProbeFacts`
    (`protocol_version`, `server_ts_us`, `geoblock`) and of `WalletConfig` (`signer`,
    `configured_signer`, `maker`, `signature_type`). `assess_account` takes the `AdmittedWallet` by
    value and gives the `WalletIdentity` of the start. `preflight_polymarket` takes `&EnvFile`.
    `ProvenSignatureType` has its merit in §8 (2026-11-16 section on the `pub` items of part 4).
    WHY: a test builds the facts and the wallet of a start by hand, from recorded answers.
- [ ] Ruling, 2026-11-15: the event funnel of a venue edge has no caller outside the tests, and it
  is `pub` by this ruling. It is `EventFunnel` with `new` (`engine::venues::edge`), the one writer
  of the input queue of a venue. It stamps each message with its venue and its queued time. A test
  sends events through `EventSink`, which is `pub` already. Test:
  `one_input_queue_with_monotone_queued_stamps`, which each venue edge runs through
  `venue_edge_keeps_each_written_obligation!`.
  - WHY: an edge must stamp each event that it queues with a time that never goes back. The seam
    suite pinned only the received stamps, and part 3 handed on the queued stamps. A test reaches
    them only with the funnel in front of a real ring.
  - The simulator edge stamps its messages itself, not through the funnel. Its queued stamp stays a
    named gap.
- [ ] This ruling makes no other item `pub`. The ban of 2026-07-28 on `pub(crate) -> pub` for a
  test holds for each other item.
- [ ] Ruling, 2026-11-17: the file steps of a write have no caller outside the tests, and they are
  `pub` by this ruling. They are the module `engine::runtime::durable_file`, with the trait
  `FileSteps` (create a folder, create a file, rename, sync a folder, remove), the trait
  `StepFile` of an open file (each write, and `sync`), and `DiskFiles`, the adapter of the real
  file system. The tape writer takes them through `PersistWriter::through(steps)`, and
  `PersistWriter::on_disk()` gives `DiskFiles`. Test:
  `kill_at_each_write_step_leaves_no_file_without_footer`.
  - WHY: the seam changes. A tape file takes its final name only when it is complete (§6,
    2026-11-17). No trick of the real file system fails a write of an open file or a sync. Thus a
    test ends or fails a write at one step only through a stand-in, the second adapter of the
    trait, in the fitness suite.
  - `WriteError`, `Staging`, and `write` stay `pub(crate)`.
  - The config edit of the fleet server and the fault of the tape writer (part 5) reach their
    faults through this seam. IF one of them needs one more item `pub`, THEN its task names the
    item and its test in its report, and the second law pass of part 5 rules on it, as built.
  - Amended 2026-11-18, as built: the ruling also makes `pub` the sixth method of `FileSteps`,
    `set_permissions`, with a default that calls `std::fs::set_permissions`, and
    `Fleet::with_file_steps(steps)`, which gives the fleet a stand-in for the steps of its config
    edit. Test: `failed_config_edit_changes_nothing`. `write_through`, `WriteFailure`, and
    `LeftBehind` stay `pub(crate)`, with `WriteError`, `Staging`, and `write`. WHY: the config edit
    keeps the permission bits of the old file, and a test fails its edit at one step only through
    the stand-in.
- [ ] Ruling, 2026-11-17: the one accept loop of the relay and the fleet server has no caller
  outside the tests, and it is `pub` by this ruling. It is the module `engine::operator::accept`,
  built when feature `web` or feature `mcp` is on: the loop, its listener trait, and the adapter
  of the TCP listener that each server passes. Test:
  `accept_errors_past_cap_give_the_class_of_the_caller`.
  - The loop tries again after an accept error, up to a cap of tries in a row, with a pause that
    doubles from 100 ms. One good accept sets the count to 0. Past the cap the loop returns its
    error, and the caller gives the class: the relay exits with a code that is not 0, and the fleet
    server takes no new session and keeps each engine that it started. An error of the loop never
    stops an engine. The exit signal of the fleet server still stops them (2026-10-14).
  - The cap is the flag `--accept-tries` of `polysim-web` and `polysim-mcp`, default 3, because
    neither binary reads a config file.
  - WHY: the seam changes. Two copies of one loop tried again with no end. A test fails an accept
    only through a stand-in listener, the second adapter of the trait. The module sits in
    `engine/src/operator/`, because `relay/` builds only with feature `web`, and `fleet/` only with
    feature `mcp`.
  - The second law pass of part 5 names each item of the module as built.
  - Amended 2026-11-18, as built: the items of `engine::operator::accept` (`pub mod`):
    - `Listener`, with `accept` and `pause` (a default of `tokio::time::sleep`), and its adapter
      `impl Listener for TcpListener`
    - `AcceptError` with `tries` and `last`
    - `each_connection(listener, tries, serve) -> AcceptError`, which returns only at the cap
    - `AcceptTries`, which `pub` has on its own merit: `polysim-mcp` parses its flag with it (§8,
      2026-11-18 list of the `pub` items of part 5)
  - Amended 2026-11-18, as built: the cap is reached at `tries` errors in a row, and no pause
    follows the last one. The pause has no ceiling and no random part. A good accept also sets the
    pause back to 100 ms. The relay exits with code 1. Past the cap, the fleet server stops its
    engines and exits 0 at its exit signal, or at the end of its stdin under `--watch-stdin`. A
    value of 0 for `--accept-tries` is refused. The L12 test module builds only with feature `web`.
  - Replaced 2026-11-18: "the one accept loop of the relay and the fleet server has no caller
    outside the tests". WHY: `AcceptTries` has one, so the ruling covers each other item.
- [ ] This ruling makes no other item `pub`. The ban of 2026-07-28 on `pub(crate) -> pub` for a
  test holds for each other item.
- [ ] Ruling, 2026-11-18: the one link credential has no caller outside the tests, and it is `pub`
  by this ruling. It is `LinkCredential` with `new(token, strategy_id)`, `token_hash`, `guard`, and
  `identity` (`engine::link`, `engine/src/link/credential.rs`). The relay takes its token hash for
  the hello of a tab, and the engine builds its guard and its identity from it. Tests:
  `tab_subscribe_is_admitted_by_the_real_link_guard`, `start_then_stop_from_a_tab_parks_the_fold`.
  - WHY: the seam changed. The token hash was derived in two places, and no test drove both. A
    test reaches the real guard of the engine only through the one constructor.
- [ ] Ruling, 2026-11-18: `Fleet::with_identity_locks(locks)` has no caller outside the tests, and
  it is `pub` by this ruling. It gives the fleet the folder of its identity locks. Each test of the
  fleet passes a folder of its own (`EngineTree::fleet` of the fitness support). Tests:
  `engine_started_elsewhere_is_listed_running_and_start_is_refused`, and each test that builds a
  fleet.
  - WHY: the folder of the host is shared by each engine of the host. A test that uses it can
    see a real engine as running elsewhere, or keep a real engine from its start. `Fleet::new`
    keeps the folder of the host.
- [ ] Ruling, 2026-11-18: the value form of the fault seam of the tape writer has no caller outside
  the tests, and it is `pub` by this ruling. It is `TapeStatus { queue, clock }`,
  `PersistWriter::reporting_to(status)`, `PersistHandle::stopped_tables()`, and `StoppedTables`
  with `count` (`engine::tape`). Test:
  `audit_write_fault_stops_quoting_and_research_fault_stops_one_table`.
  - WHY: the class of a tape fault is a seam that changed in part 5. The writer runs on its own
    thread against the file system, so a test drives it with a stand-in of the file steps, gives it
    a status queue, and reads its count of stopped tables, as with the value form of the exposure
    writer seam (2026-11-13 ruling).
- [ ] Ruling, 2026-11-18: the read accessors of part 5 that only tests call from outside the crate
  are `pub` by this ruling: `ControlPlane::desired` and `ControlPlane::acknowledged`,
  `ControlQueueProducer::dropped`, `HistoryRing::{held, capacity, len, is_empty}`, and
  `SenderCounts::of`.
  - WHY: each reads a state, a count, or a size that a contract or fitness test of part 5 asserts,
    and writes nothing. The value lives only in its owner, or in a log line, which a test cannot
    assert.
  - Tests: `acknowledged_state_reaches_highest_epoch_after_drops_stop`, the control case of
    `each_lane_full_gives_its_class`,
    `late_subscriber_gets_recent_events_and_live_frames_still_flow`,
    `history_ring_never_grows_and_never_reaches_the_hot_thread`, and
    `link_drops_are_counted_for_each_sender`.
- [ ] This ruling makes no other item `pub`. The ban of 2026-07-28 on `pub(crate) -> pub` for a
  test holds for each other item.

### Amended 2026-11-13: what an agent runs against a venue

WHY: the owner takes no part in a build (live policy). A plan or a skill can still give a run at a
venue to a person. Thus each run against a venue needs one owner, in one table.

| Run | Who | How |
| --- | --- | --- |
| Each test of target `fitness`: lanes `fitness` and `contract` | each agent | gate G step 4. Each venue there is a stand-in or a recorded payload |
| Target `integration` | each agent compiles it | gate G step 4. Each of its tests is `#[ignore]`d, so none runs |
| A public read of target `integration` | an agent, only where a plan names it | by its exact name, with an empty environment (top of §12) |
| A signed test, its dry-run form, its kill switch, and its rollback | only `live-runner` | by the runbook of its job and the live policy |
| Each other live run: the probe, an engine with execution `live` | only `live-runner` | by the live policy |
| A run that a plan or a skill gives to a person | no lane of a person | the live policy decides (below) |
| Amended 2026-11-15: the live capture of Polymarket, `poly_rotation::poly_rotation_capture_spans_two_rotations_with_zero_gap_handover`: a public read | only the verifier of the job whose plan names it | by its exact name, from an empty folder outside each checkout (top of §12). Its environment is empty except `PATH`, `HOME`, and `TMPDIR`. It loads no key, sends no signed request, and writes only below `TMPDIR` |
| Amended 2026-11-15: the signed tests of Polymarket in `engine/tests/integration/poly_live/`: one signed order (`venue_accepts_one_signed_order`), a cancel beside a held placement (`cancel_finishes_while_a_marketable_place_is_held`), the kill switch, the rollback, and the reads, each with its dry-run form | only `live-runner` | by the runbook of its job and the live policy, at the live venue. No testnet, and no lane of a person (below) |
| Amended 2026-11-15: the silent stream of Binance spot, `binance_silence::silent_user_stream_still_answers_reads`, with its dry-run form | only `live-runner` | by the runbook of its job and the live policy, at the live venue. No testnet, and no lane of a person. The tests of `binance_recording.rs` are its kill switch, its rollback, and its reads |

- [ ] A live run = a process that sends a signed request to a venue (2026-11-12).
- [ ] The run command of a signed test stands only in the runbook of the job that runs it. A
  README, a comment, or an ignore reason never names it (2026-11-12).
- [ ] IF a plan or a skill gives a run at a venue to a person, THEN the live policy decides.
  `live-runner` does the run, or nobody does it, and the hand-off file of the job says so.
- [ ] CAUTION: Do not start quoting again after a quoting halt, and do not start an engine again
  after a refused start. §6 keeps each of them for a person, and no row of this table gives them
  to an agent. A restart can send orders from a state that the halt found unsafe.
- [ ] Amended 2026-11-13, as built: the signed tests of
  `engine/tests/integration/binance_recording.rs` are the recording, the kill switch, the rollback,
  and the reads of the venue, each with its dry-run form. Each dry run loads the live keys, so it is
  a live run too. Only `live-runner` runs it, and the ledger of the live policy gets a row for it.
  WHY: a dry run that loads a live key can lose that key as a live run can.
- [ ] Amended 2026-11-15: the tests of part 4 against a venue, the last three rows of the table.
  WHY: the plan of part 4 gives its signed tests to a person, and one of them to a testnet. The live
  policy gives each one to `live-runner`, at the live venue.
  - Each dry run of `poly_live/` and `binance_silence.rs` loads the live keys. The line above on
    the dry runs of `binance_recording.rs` binds it.
  - The kill switch of Polymarket cancels only the venue order ids that the placement answers of its
    tests gave. A test writes each id to the id file of its run folder before its next step. No
    cancel names the account, the market, or the token alone (2026-11-12 CAUTION on the kill
    switch).
  - The job of part 4 runs neither `cancel_finishes_while_a_marketable_place_is_held` nor its
    rollback `sells_back_the_shares_of_the_held_placement`. Both stay in the code, each with its
    dry-run form and its runbook section. The live policy permits one signed order test at
    Polymarket and one order for each side. `venue_accepts_one_signed_order` is that test.
  - Amended 2026-11-16, as built: the signed tests of Polymarket sit in the folder
    `engine/tests/integration/poly_live/`. `cancel_finishes_while_a_marketable_place_is_held`
    refuses in code under the caps of the live policy: its live form stops at the cap of one order
    for each side, before any order action. Its dry run builds and prints its requests and sends
    none. WHY: a cap in code stops a run that a runbook step lets through by mistake.
  - Replaced 2026-11-16: "`engine/tests/integration/poly_live.rs`" in the second of the three rows
    and "`poly_live.rs`" in the line on the dry runs. WHY: the tests sit in a folder of three files.
  - Amended 2026-11-16, as built: each signed test of Polymarket, its dry run too, asks the venue
    to derive the L2 credentials of its wallet before the start gate. IF the venue derives none,
    or IF the gate then holds credentials that are not the derived ones, THEN the test stops
    before any request of the tests. A test that places an order refuses before it signs the
    order, unless the geoblock flag of the venue reads `blocked = false` for this host. WHY: the
    start gate creates credentials when the venue derives none, a change of key settings that
    the live policy forbids. The venue refuses an order by location.

## 13. Performance

- [ ] Design principles protect performance, not a standing benchmark mandate. Principles: zero
  steady-state alloc, contiguous fixed layouts, integer keys, no strings/formatting/syscalls on hot
  path.
- [ ] Review rejects premature optimization.
- [ ] IF performance question arises, THEN profile first. Act on evidence.
- [ ] Predictable performance beats peak performance.
- [ ] Reader must infer hot-path cost model from source.
- [ ] Branch temperature: declared, not guessed.
  - Amended 2026-07-18. WHY: hot/cold split + inlining hints are design, not premature
    optimization.
  - `#[cold]`: on failure/drain/reconnect/capacity-hit functions reachable from hot path.
  - `#[inline]`: on small hot accessors + newtype ops.
  - `#[inline(always)]`: sparingly. Tiny leaf functions on proven hot path only. Inline WHY
    required.
- [ ] Cache-line layout on hot structures where it makes sense.
  - Amended 2026-07-18. WHY: predictable memory movement is §3's whole point.
  - Group hot fields.
  - `#[repr(align(64))]` / padding against false sharing on cross-thread-touched state.
  - Contiguous arrays over pointer-chasing.
  - Layout choice = visible in review, not accidental.

## 14. Toolchain & Gates

- [ ] Toolchain: `1.92.0`, pinned in `rust-toolchain.toml` with `rustfmt`, `clippy`, and target
  `wasm32-unknown-unknown`. Edition 2024. Upgrades deliberate: a new toolchain = an amendment of
  this clause.
- [ ] Gate G: gate every change with six steps, run at the workspace root. All green or no merge:
  1. `cargo fmt --all -- --check`
  2. `cargo clippy --workspace --all-targets -- -D warnings`, then
     `cargo clippy --workspace --all-targets --all-features -- -D warnings`. Both read the clock
     lint of `clippy.toml`.
  3. `cargo clippy --target wasm32-unknown-unknown -p engine --lib --bin polysim-ui --bin dom-fixture -- -D warnings`
  4. For each package and each test target of its manifest:
     `cargo test -p <package> --test <target>`, then the same commands with `--all-features`.
     Today only package `engine` has test targets:
     - `cargo test -p engine --test fitness`
     - `cargo test -p engine --test integration`
     - `cargo test -p engine --all-features --test fitness`
     - `cargo test -p engine --all-features --test integration`
  5. `cargo test -p engine --release --test fitness step_allocates_nothing`
  6. `(cd browser && trunk build --release) && (cd tools/dom-fixture && trunk build --release)`,
     then
     `docker buildx build --platform linux/arm64 --build-arg BIN=strat-micro-recorder-te-binance-spot-btcusdt --output type=local,dest=dist .`
  - Amended 2026-11-12, clock lint and release proof. WHY: a clock read that reaches engine state,
    and an allocation that only the release optimizer adds or removes, each passed a debug gate.
    The lint and a release run of the zero-allocation proof make each of them a red step.
    - [ ] Step 2 reads `clippy.toml` at the workspace root. `[workspace.lints.clippy]
      disallowed_methods = "deny"` of the root manifest makes each banned call an error in each
      crate of the workspace.
    - [ ] The ban holds each call that reads a clock or a counter, or that turns a counter tick
      into time: the std, wasm, tokio and `quanta` clocks, and the clocks of the engine
      (`EngineClock::start`, `EngineClock::now`, `wall_clock_now`, `Stamper::stamp`, and the tick
      scale).
      - Replaced 2026-11-13: "`boot_stamp_us`" in that list. WHY: the function got the name
        `wall_clock_now`, which says what it reads (§5, 2026-11-13).
    - [ ] Each permitted site carries `#[expect(clippy::disallowed_methods, reason = "...")]`. An
      `#[expect]` that meets no read fails step 2. The fitness test
      `permitted_clock_sites_equal_the_list` holds the list of permitted sites, and each banned
      engine path keeps at least one site there. A new site needs its reason and its line in that
      list. No production root holds an `allow` of the lint.
    - [ ] The two test targets hold one `allow` each, with its reason: a test deadline is never
      engine state.
    - [ ] Step 5 runs the zero-allocation proof of the hot driver in the `release` profile. Its
      name filter selects one test. A filter with no match passes with 0 tests, so the
      suite-integrity test holds that name (`FILTERED_TEST_NAMES`): a rename fails step 4.
    - [ ] The last task of each part also runs the proof one time in profile `deploy`, the profile
      that ships: `cargo test -p engine --profile deploy --test fitness step_allocates_nothing`.
      It is no gate step: a build with full link-time optimisation takes minutes.
    - [ ] The Trunk and Docker step that the amendments below call step 5 is step 6 from this
      date.
    - Replaced 2026-11-12: gate G had five steps, and step 5 was the Trunk and Docker step. A word
      scan of four folders held the clock rule.
  - Amended 2026-11-12, named test targets. WHY: `cargo test --workspace` passes when a test target
    goes from a manifest, and the guard of the suite goes with the target that it guards. A named
    target that is missing makes cargo 1.92.0 exit 101 ("error: no test target named ..."), so the
    gate sees a lost suite.
    - [ ] Step 4 names each test target of each package. A change that adds a `[[test]]` target
      adds its two commands to step 4, here and in `AGENTS.md`, in the same change.
    - [ ] Packages `lab` and `strat-micro-recorder` have no test target today.
    - [ ] Target `integration` only compiles in step 4: each of its tests is `#[ignore]`d (§12).
    - [ ] Step 4 runs no lib, bin or doc test. This law has no unit tier and no doc tier (§12), and
      step 2 still compiles each target. A task that writes an inline scaffold (§12, 2026-11-12)
      runs it by its own command inside that task.
    - [ ] CAUTION: Delete or rename a test target only in the same change as its step-4 commands.
      The `engine` manifest has no `autotests = false`, so Cargo still finds target `fitness` from
      `engine/tests/fitness/main.rs` when its `[[test]]` table is gone.
    - Replaced 2026-11-12: step 4 was `cargo test --workspace`, then
      `cargo test --workspace --all-features`.
  - Amended 2026-11-12. WHY: the workspace holds three packages, so each command names the
    workspace or package `engine`. Step 5 holds the two Trunk builds and the deploy build of one
    engine binary: only they prove the two pages, `Dockerfile`, and the `deploy` profile of the
    root manifest. The toolchain stays `1.92.0`: a new compiler in the same change as the
    workspace split gives one red gate two causes.
  - [ ] Step 4 runs no ignored test. No agent adds a flag that runs one (§12).
  - [ ] Step 6 passes at exit 0, with the file `dist/strat-micro-recorder-te-binance-spot-btcusdt`.
  - Amended 2026-10-26. WHY: the browser workstation compiles only for wasm32, so a gate that
    lints only the native target never reads it. `--all-features` (`mcp` + `web`) covers the
    gated native paths. A single-config gate rots the path it never compiles.
  - Replaced 2026-11-12: the gate was six commands of steps 1 to 4 in single-crate form, with no
    `--workspace`, no `--all`, and no `-p engine`, and the two Trunk builds sat outside the gate.
  - Replaced 2026-10-26: the gate was `cargo fmt --check` + clippy + `cargo test`, with clippy and
    test run in BOTH configurations: no features, and `--all-features` (`ui` + `mcp`).
    - Amended 2026-07-25. WHY: `eframe` is optional behind feature `ui` (§11), and a
      single-config gate rots the path it never compiles.
    - Amended 2026-10-14. WHY: `mcp` joins `ui` as an optional feature, and one `--all-features`
      run compiles both gated paths.
    - Replaced 2026-10-14: the second configuration was `--features ui`.
- [ ] `#[allow]` requires inline WHY.
- [ ] `#![forbid(unsafe_code)]` at the root of each lib crate (§8, 2026-11-12 section on two lib
  crates). An amendment requires a benchmark that proves safe implementation cannot meet measured
  requirement.

## 15. Amendments

- [ ] Changes only by explicit agreement, never silent.
- [ ] Every amendment carries one-line WHY.
- [ ] PROJECT.md, PLAN.md, CHECKLIST.json must comply. IF they cannot, THEN surface conflict +
  resolve it here first.

## 16. Smell Doctrine

Added 2026-08-07 (user directive). Operative playbook = the `rex-code-smells` skill. This section
is its constitutional core.

- [ ] Detection ≠ fixing.
- [ ] A finding = file:line + evidence + concrete cost + one-line fix direction.
- [ ] Severity, three only: BLOCK (bug factory) / REFACTOR (taxes every future edit) / NIT (real
  but cheap).
- [ ] Sweeps (grep, clippy) yield CANDIDATES. Only reading in context confirms. One false positive
  poisons the whole report.
- [ ] Clean is a valid verdict. A fabricated finding costs more trust than an empty report.
- [ ] Naming lens:
  - booleans are predicates
  - conversion prefixes tell the truth (`as_` free view, `to_` does work, `into_` consumes)
  - no noise words
  - domain primitives are newtyped, so a swapped argument cannot compile
- [ ] Brittleness lens:
  - `unwrap`/`expect` never meets external input
  - knowledge is encoded once: the same magic literal in two places is a defect
  - ordering is enforced by types, not asserts
  - every seam needs its writer AND its reader: data without control flow compiles green and lies
  - tests pin behavior, never formatting
- [ ] Structure lens:
  - the deletion test (§1) kills speculative generality
  - a long function is N jobs sharing one name
  - an `Option` field alive in only one phase is a missing enum state
  - the same match in three files is one change costing three edits
