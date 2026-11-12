# Tools

Small standalone tools that use the library, built because they were needed while developing it, and kept because they stay useful. `poly-probe` is a Cargo example, so it runs with `cargo run --example poly-probe`. `dom-fixture` is a bin: Trunk builds only bins, and it is a browser page.

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

A read-only health check for a Polymarket account. It answers, in one report: is this host geoblocked, do the credentials in `.env` actually work, what kind of wallet is this, what does the account hold, and what orders are resting. It never places, changes, or cancels anything — run it as often as you like, and always before trusting any execution setup.

```shell
cargo run --example poly-probe              # reads .env
cargo run --example poly-probe path/to/env  # or a specific file
```
