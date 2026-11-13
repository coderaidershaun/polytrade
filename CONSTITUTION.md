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
- [ ] The execution lease is the last check. So each other refusal leaves the run nonce unspent.
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
- [ ] IF a pin is refused (Linux), THEN each thread that started stops before the start returns.
  No writer has started, so no tape file is pruned. The run nonce is spent by then.
- [ ] The pin call is a parameter (`PinCall`). `run_trading_engine` passes `pin_to_core`, and a
  test passes a stand-in.

### Amended 2026-11-13: the OTLP check and the exporter

WHY: the export adds one start check and one thread, and neither may change the order of the pins.

- [ ] Preflight checks the `otlp:` block (feature `otlp`) after the core id and before the scale
  read: a collector on loopback, and room for the stop of the exporter inside
  `engine.drain_deadline_ms`. It starts nothing, and it reads no network.
- [ ] The exporter thread does not pin, so it is not in the pin order. Its lanes open right after
  the log starts. The thread starts last, after the link, when no refusal can follow. So no refused
  start has an exporter to stop.
- [ ] IF the OS refuses the exporter thread, THEN the run goes on with no export and one ERROR line.
  The start never panics for the export, so the drain and the exit sweep always run.

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
    one of the two ring-pressure halts (2026-11-13 section on failure classes, below). The bank is
    full when it holds `COMMAND_BANK_CAPACITY` commands, never at the capacity of its `Vec`.
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
  - Amended 2026-11-13, as built: only `StartError`, `ConfigError`, and `FatalReason` have
    `failure_class()`. Four error enums reach the drain of the start code with none:
    `PersistError`, `ExposureError`, `SweepFailure`, and `DrainFailure`. The next part of the plan,
    part 3 (money), closes the gap. Until then this clause is the rule, not yet a fact of the code.
- [ ] An error that wraps another error gives the class of the inner error, or a higher class.
  Never a lower one.
- [ ] One supervisor acts on the class at each thread root: the driver of the hot thread, the
  session chassis of a venue edge, the root of each writer thread, and the start code. Lower code
  returns the error.
  - Amended 2026-11-13, as built: no supervisor reads `failure_class()` yet, and only fitness tests
    read it. Each of the 19 sites that trip the fatal latch trips it itself, where the error is
    found. The next part of the plan, part 3 (money), closes the gap. Until then this clause is the
    rule, not yet a fact of the code.
- [ ] An internal invariant violation has no failure class. It is a bug, and it panics (top of §6).

Lanes:

- [ ] Each lane type names its failure class in code, beside the type:

| Lane | When the ring is full | Class |
| --- | --- | --- |
| Engine-fed input queue: market data (`queues.input_capacity`), timer (`queues.timer_capacity`), execution events (`queues.execution_capacity`) | fatal latch, bounded drain | HALT (engine) |
| Market tap ring of the simulator | fatal latch, bounded drain | HALT (engine) |
| Link input queue (`queues.link_capacity`) | drop the frame, count it (2026-07-25) | REJECT |
| State lanes: UI books and events, outbound link, strategy log, latency samples | drop the record, count it | REJECT |
| Exposure lane | keep the newest snapshot | REJECT (of the older snapshot) |
| Command lane | commands stay in the bank, in order, with their bank stamps (2026-07-27) | RETRY |
| Command bank | quoting halt `HaltReason::CommandBankOverflow` | HALT (quoting) |
| Persistence seal | try at the request and at each spin, up to `persistence.seal_try_limit` tries, then quoting halt `HaltReason::SealNotAccepted` | RETRY, then HALT (quoting) |
| Copy lane of the OTLP export (feature `otlp`): each line that the log drain writes | drop the copy, count it (2026-11-13, the OTLP export) | REJECT |
| Rows lane of the OTLP export (feature `otlp`): each row of the metrics thread | drop the row, count it (2026-11-13, the OTLP export) | REJECT |

Two kinds of halt:

- [ ] Quoting halt (`HaltReason`): quoting stops, and the engine sweeps its own orders. State stays,
  and the process stays up. Only a person starts quoting again.
- [ ] Engine halt (`FatalReason`): the fatal latch trips with a typed reason, and the first reason
  stays. The drain runs inside its deadline, and the exit code is not 0.
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
- [ ] Public paths: `polysim::…` = `engine::…`. An exported macro sits at the crate root
  (`engine::info!`). Each other path follows its folder: `engine::venues::sim::…`,
  `engine::tape::…`, `engine::runtime::config::…`.
- [ ] `engine::runtime::{StartError, ExecutionLease, Exit, check_symbol_order_capacity,
  run_trading_engine}` keep their path: the root file of `runtime` exports them from `assembly`,
  with the start and exit items of 2026-11-13 (`Preflight`, `StartPlan`, `Engine`, `DrainOutcome`,
  `Trigger`, `DrainFailure`, `DrainDeadline`, `DrainWatchdog`, `ForcedExit`).
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
- [ ] The task that deletes a test records it in the hand-off file of its job: one table row with
  the old test, the new test that pins its behavior, and the module. The hand-off file also holds
  the list of tests (`cargo test -- --list`) before and after.
- [ ] The suite-integrity test of `engine/tests/fitness/scans/` holds during each deletion: a file
  with no `mod` line fails it, and a file that goes with its `mod` line passes.

### Amended 2026-11-12: a seam that changes gets a contract test

WHY: the link token hash is derived in two places, the relay and the engine guard, and no test
drives both. A seam that a change reshapes and that no test drives can break with each gate green.

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
- [ ] Each other item of 2026-11-13 that a contract test reaches has a caller outside the tests,
  and §8 names it (2026-11-13 section on the fold, start, and exit seams).
- [ ] This ruling makes no other item `pub`. The ban of 2026-07-28 on `pub(crate) -> pub` for a
  test holds for each other item.

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
