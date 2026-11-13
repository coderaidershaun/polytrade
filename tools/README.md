# Tools

Small standalone tools that use the library, built because they were needed while developing it, and kept because they stay useful. `poly-probe` and `latency-probe` are Cargo examples. `dom-fixture` is a bin: Trunk builds only bins, and it is a browser page.

## dom-fixture

The workstation panels on fake data. It shows the same panels the real workstation shows — price ladder, charts, monitor, positions — but fed from canned scenes instead of a live exchange, so you can check that a UI change looks right without starting an engine or connecting to anything.

```shell
(cd tools/dom-fixture && trunk serve)   # then open the URL it prints
```

Press 1–9 to flick between ladder scenes. Arrow up and down step the scene, arrow left and right step its variant. M steps the monitor scene and T its tab. C steps the chart scene and V flips line and candles. P steps the risk scene and X flips exposure and PnL.

The starting selection comes from the page's query string, such as `?DOM_SCENE=3&DOM_LEVELS=40`. A value that is not a number is ignored. A scene or variant number past the end picks the last one.

| Parameter     | Picks                                                                             |
| ------------- | --------------------------------------------------------------------------------- |
| `DOM_SCENE`   | ladder scene, counted from 1                                                      |
| `DOM_VARIANT` | variant of that scene, counted from 0                                             |
| `DOM_LEVELS`  | ladder rows per side (12 by default). No key does this                            |
| `MON_SCENE`   | monitor scene, counted from 1                                                     |
| `MON_TAB`     | monitor tab, counted from 0: System, Orders, Pub trades, Trade fills              |
| `CHART_SCENE` | chart scene, counted from 1                                                       |
| `CHART_MODE`  | `candles`, or anything else for the line                                          |
| `RISK_SCENE`  | risk scene, counted from 1. It also puts the risk view on the left, not the chart |
| `RISK_SERIES` | `pnl`, or anything else for exposure                                              |

A plain `cargo run --bin dom-fixture` has no screen. It prints the risk scenes as text instead, one summary per scene, which proves the series without a browser.

## poly-probe

A read-only health check for a Polymarket account. It answers, in one report: is this host geoblocked, do the credentials in `.env` actually work, what kind of wallet is this, what does the account hold, and what orders are resting. It never places, changes, or cancels an order. But it reads the wallet key from `.env`, signs its requests, and can create an API key on the account. Only the `live-runner` role of a job runs it, by a runbook and the live policy.

## latency-probe

The timing harness. It measures the latency of the engine without changing it, in four modes:

| Mode | What it times | Connects to |
| --- | --- | --- |
| `step` | Each call of the hot loop's `HotDriver::step` that folds a recorded trade, with the driver's stamps on or off, so the cost of measurement shows | nothing |
| `sender` | Each command from the moment it enters the lane to the moment a loopback socket takes its bytes, with the sender `shared` or `pinned` | a loopback socket |
| `idle` | How long a message waits in its queue while the hot thread spins or parks between messages | nothing |
| `stages` | Each stage of a full engine start on public Binance data, written to the engine's log once a minute | the venue, for public data only |

Each line it prints gives p50, p99, p99.9, max, the count, and the drops and backward pairs, never a mean. A number from a machine that is not the trading host is for development only.

Modes `step`, `sender` and `idle` open no connection to a venue and read no `.env`. Mode `stages` is a full engine start: its config, `tools/latency-probe/stages.yaml`, runs with execution off, and the harness refuses a config with execution live before it builds anything. Build it with the `deploy` profile, and run it from a folder outside the checkout that holds no `.env`. The runbook of the latency proofs gives each command.
