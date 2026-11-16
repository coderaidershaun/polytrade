# Integration tests: runbook sections

Each test of this folder is ignored. The signed tests of `binance_recording.rs` and `binance_silence.rs` use real funds on live Binance spot, BTCUSDT. The signed tests of the folder `poly_live/` use real funds on live Polymarket, in one window of `btc-updown-5m`.

Only the role `live-runner` runs a test of this file, by the runbook of its job and the live policy. The run command of each test is in the runbook of the job, and in no other place.

Each dry run of this file loads the live keys, and most of them make signed reads. So each dry run is a live run by the live policy. Only `live-runner` runs it, and each dry run gets its own row in the ledger. A dry run sends no order action.

## Words

- **Run folder:** the folder that the variable `REX_RUN_DIR` names. One live run has one run folder, below the data folder of its job. It is never below `engine/fixtures/`.
- **Order of the tests:** an open order whose client order id passes the ownership rule of the codec with the tag of these tests. Its id starts with `pd-` and that tag.
- **Caps:** the caps of the live policy for Binance spot BTCUSDT:
  - one order: 10 USDT
  - the ceiling of one order: 25 USDT
  - exposure: 50 USDT
  - orders for each side: 1
  - the reserve, which no trade reaches: 0.0002 BTC and 20 USDT
  - the length of one run: 10 minutes

## Preconditions of each signed test

These are steps 1 to 3 of `## Shape of a live run` of the live policy.

- [ ] You hold the lock of the live policy, and the ledger shows that the loss budget of the build is not spent.
- [ ] The key file gives the keys by one of the two ways of the live policy. No key is in a file of the repo.
- [ ] The dry-run form of the test ran first, and its output is as its section says.
- [ ] `writes_the_venue_reads` wrote `before.json` in the run folder. The clock skew is inside 1 second.
- [ ] `before.json` shows no open order that this build did not place. IF it shows one, THEN do not run. Do not touch that order. Report it.

## `writes_the_venue_reads`

Only `live-runner` runs this test, by the runbook of its job.

- **Dry run first:** `writes_the_venue_reads_dry_run`. It loads the keys, signs one account read, and prints it with `apiKey` and `signature` as `<redacted>`. It sends nothing and writes nothing.
- **What it does:** it reads the clock skew, the open orders of BTCUSDT, and the balances of BTC and USDT. Then it writes them to the file that `REX_READ_PHASE` names (`before.json`, `after.json` or `after-rollback.json`) in the run folder. It never writes over a file.
- **Expected outcome:** one new file. The test prints the count of open orders, the count of the orders of the tests, and the clock skew.
- **Check at the venue:** the open orders and the balances on the web page of the account are the same as in the file.
- **Kill switch:** `cancels_only_the_orders_of_the_tests`. **Rollback:** none. The test sends no order.

CAUTION: The files of this test hold balances. Do not put them in Git or in a hand-off file. They go when the job tears down its data.

## `records_a_small_order_lifecycle_on_binance`

Only `live-runner` runs this test, by the runbook of its job.

- **Dry run first:** `records_a_small_order_lifecycle_on_binance_dry_run`. It reads the venue and builds the buy and its cancel. Then it prints each request with `apiKey` and `signature` as `<redacted>`. It sends no order action and writes no file.
- **Dry run, more:** it also builds the taker buy of the fallback below, and sends none. It prints `dry run: 3 requests built, none sent`.
- **What it does:**
  1. It places one post-only buy of about 10 USDT at the best bid, under the tag of the tests.
  2. It records each answer, and each execution report of the user data stream, in `recording/` of the run folder.
  3. It waits up to 5 minutes for a fill. Then it cancels what rests, by its client order id.
     - IF the venue refuses the buy because the buy would cross (HTTP 400, code -2010, `Order would immediately match and take.`), THEN no order rests. The test does not wait, and it sends no cancel. It prints `the venue refused the post-only buy, because it would cross: no fill`. The line `placed:` and the line of that order under `what the venue gave:` then show `unknown (...)` with code -2013, `Order does not exist.`: the venue holds no such order.
  4. IF the buy has no fill, THEN the test reads the touch, the account and the open orders again. The buy has no fill when its 5 minutes end and the venue shows its cancel, or when the venue refuses it as in step 3. IF a buy of the tests rests, or a cap refuses, THEN the test stops and sends nothing. ELSE the test sends one buy of the same size at the best ask, with time in force IOC. It sends this buy once, on the same connection. The buy lives for one match and never rests. The test records its answer and its trade reports in the same way.
- **Expected outcome:**
  - A resting order is placed and cancelled, or it fills, or the venue refuses it because it would cross. Then there is at most one taker fill.
  - The run ends inside 10 minutes.
  - At no time does more than one order of the tests rest on a side. At the end, no order of the tests rests.
  - The test prints what the venue gave, and the fee fields `t`, `l`, `z`, `n`, `N` of each trade report.
  - No payload file holds a key, a signature, a listen key, an account id, or a balance event.
- **Expected cost:** one taker fee and the spread on about 10 USDT. The rollback then sells the base back to `before`, within one lot.
- **Check at the venue:** the order history of the account shows the buy of the tests (filled or cancelled), and at most one taker buy of the tests. A buy that the venue refused because it would cross can be missing there.
- **Kill switch:** `cancels_only_the_orders_of_the_tests`. **Rollback:** `sells_back_the_base_above_before`. Run the rollback after each fill.

The test refuses each of these in code, before it sends an order:

- [ ] a symbol other than BTCUSDT
- [ ] an order above 10 USDT, or a ceiling above 25 USDT
- [ ] an exposure above 50 USDT
- [ ] an order of the tests that already rests on the buy side
- [ ] a buy that takes the free USDT below 20 USDT

A request with no answer is not sent again. The test reads the order at the venue to learn what happened.

## `cancels_only_the_orders_of_the_tests`

Only `live-runner` runs this test, by the runbook of its job.

- **Dry run first:** `cancels_only_the_orders_of_the_tests_dry_run`. It reads the open orders and prints each cancel that it would send. It sends no cancel.
- **What it does:** it reads the open orders of BTCUSDT. It passes each client order id through the ownership rule of the codec, with the tag of the tests. It sends one cancel for each order of the tests, by its client order id. It prints each order that it left.
- **Expected outcome:**
  - Each order of the tests is cancelled.
  - Each other order rests.
  - No request names the symbol alone.
- **Check at the venue:** the open orders of the account hold no order of the tests.
- **Rollback:** `sells_back_the_base_above_before`, when a fill bought base.

CAUTION: This test never cancels each order of the account. An engine never touches an order that it did not place. IF the test prints an order that it left, THEN do not cancel that order. Report it.

## `sells_back_the_base_above_before`

Only `live-runner` runs this test, by the runbook of its job.

- **When:** run it when `after.json` shows more free BTC than `before.json`, by more than one lot.
- **Dry run first:** `sells_back_the_base_above_before_dry_run`. It reads the account and builds the sell. Then it prints the sell with `apiKey` and `signature` as `<redacted>`. It sends nothing.
- **What it does:** it reads `before.json` and one new account read. It sends one limit sell of the free BTC above `before`, 20 ticks below the best bid, so that it takes the bid at once. The sell is at most 10 USDT. IF part of the sell rests, THEN the test cancels that part by its client order id. IF the BTC above `before` is zero or below the minimum of the venue, THEN it sends nothing and prints why.
- **Expected outcome:**
  - The free BTC equals `before.json` within one lot.
  - The test sent one order at most, and no cancel of another order.
- **Check at the venue:** the order history shows one sell of the tests. The free BTC is back to `before`.
- **Kill switch:** `cancels_only_the_orders_of_the_tests`.

The test refuses each of these in code:

- [ ] a symbol other than BTCUSDT
- [ ] a sell above 10 USDT, or a ceiling above 25 USDT
- [ ] a sell of the tests that already rests
- [ ] a sell that takes the free BTC below `before` or below 0.0002 BTC

## When the start refuses an exposure file of version 2

This section has no test.

CAUTION: Do not start the engine again after a refused start. A start again can send orders from a state that the refusal found unsafe.

1. Stop at the refusal. Do not delete, move, or change the exposure file.
2. Write down what the file holds for a person to read:
   - its path and its version
   - each row: the symbol, the position, the cash and the basis
3. Do not start the engine again.
4. Report the refusal and the notes of step 2 to the lead.

## Polymarket: words and caps

The signed tests of `poly_live/` use real funds on live Polymarket, on the series `btc-updown-5m`. Only the role `live-runner` runs one, by the runbook of its job and the live policy. Each dry run of these tests loads the live keys and runs the start gate of the engine, which makes signed reads. So each dry run is a live run, and each gets its own row in the ledger. A dry run sends no order action.

Two checks of each test, its dry run too:

- **Derived credentials only:** before the start gate, the test asks the venue to derive the L2 credentials of the wallet. IF the venue derives none, THEN the test stops before the gate, because the gate would then create credentials, a change of key settings that the live policy forbids. After the gate, IF the gate holds credentials that are not the derived ones, THEN the gate created them, and the test stops before any request of the tests. Report each stop. The test prints `l2 credentials: the derived ones`, and never a key.
- **Geoblock flag:** after the gate, the test reads the geoblock flag of the venue for this host and prints `blocked = false`, `blocked = true`, or `unread`. It never prints the address, the country, or the region of the answer. A test that places an order refuses before it signs the order unless the flag reads `blocked = false`. Its dry run prints where the real run stops and goes on, because it sends nothing. The reads and the kill switch print the flag and go on.

- **Token of the tests:** the Up token of the window that is open when `writes_the_polymarket_reads` reads `before`. Each later test of the run uses this token only.
- **Id file:** `poly-order-ids.txt` in the run folder. Its first line names the token of the tests. Each placement of a test writes a `pending` line before it is sent. Then the venue order id of its answer takes the place of that line.
- **Order of the tests:** an open order whose venue order id is in the id file. The venue accepts no client order id, so this is the only way to know an order of the tests.
- **Caps:** the caps of the live policy for Polymarket:
  - one order: 5 shares
  - the ceiling of one order: 6 USD
  - exposure: 6 USD
  - the loss budget of one run: 5 USD
  - orders for each side: 1
  - the reserve, which no trade reaches: 65 USD
  - the length of one run: 10 minutes

CAUTION: No test of `poly_live/` cancels by market, by token, or by account. Each cancel names one venue order id of the id file. An order with no line in the id file is never touched.

## Preconditions of each signed Polymarket test

These are steps 1 to 3 of `## Shape of a live run` of the live policy.

- [ ] You hold the lock of the live policy, and the ledger shows that the loss budget of the build is not spent.
- [ ] The key file gives the keys by the working folder of the live policy. No key is in a file of the repo.
- [ ] The dry-run form of the test ran first, and its output is as its section says.
- [ ] `writes_the_polymarket_reads` wrote `before.json` and started the id file in the run folder.
- [ ] `before.json` shows no open order of the account on the token of the tests. IF it shows one, THEN do not run. Do not touch that order. Report it.
- [ ] `before.json` shows at least 65.05 USD of collateral.
- [ ] More than 90 seconds of the window of the token are left. ELSE the test refuses, and the run starts again with a new run folder.

## `writes_the_polymarket_reads`

Only `live-runner` runs this test, by the runbook of its job.

- **Dry run first:** `writes_the_polymarket_reads_dry_run`. It runs the start gate. Then it prints one read of the open orders with its signed headers, each secret shown as `<redacted>`. It writes nothing.
- **What it does:** it runs the start gate, which reads the clock offset. For `before` it starts the id file with the Up token of the open window. It reads the open orders of the token, the collateral, and the shares of the token. Then it writes them to the file that `REX_READ_PHASE` names (`before.json`, `after.json` or `after-rollback.json`) in the run folder. It never writes over a file.
- **Expected outcome:** one new file with only the order id, side, price, size and status of each open order, the collateral and the shares, and the clock offset. The test prints the count of open orders, the count of the orders of the tests, and the clock offset.
- **Check at the venue:** the open orders and the balance on the web page of the account are the same as in the file.
- **Kill switch:** `cancels_only_the_orders_of_the_polymarket_tests`. **Rollback:** none. The test sends no order.

CAUTION: The files of this test hold balances. Do not put them in Git or in a hand-off file. They go when the job tears down its data.

## `venue_accepts_one_signed_order`

Only `live-runner` runs this test, by the runbook of its job.

- **Dry run first:** `venue_accepts_one_signed_order_dry_run`. It runs the start gate and reads the venue. Then it builds the buy and its cancel, and prints each request with `signature`, `owner`, `maker` and `signer` as `<redacted>`. It sends no order action and writes nothing.
- **What it does:**
  1. It derives the L2 credentials of the wallet, then runs the start gate of the engine: the wallet, the protocol version, the local clock, and the account. It makes sure that the gate holds the derived credentials, and reads the geoblock flag. It prints the wallet type, the clock offset, and the flag.
  2. It reads the book of the token. IF the best bid is 0.02 or less, THEN it refuses, because a buy at 0.01 would sit at the touch.
  3. IF the geoblock flag does not read `blocked = false`, THEN it refuses here, before it signs the buy. ELSE it writes a `pending` line to the id file. Then it sends one post-only buy of 5 shares at 0.01, signed by the encoder and the signer of the engine.
  4. The venue answers with a venue order id. The test writes that id to the id file in place of the `pending` line.
  5. It reads the open orders of the token: the buy rests.
  6. It sends one cancel of that venue order id. Then it reads the open orders again, and prints each order that it left.
- **Expected outcome:** one order placed and cancelled by its own id. The buy costs at most 0.05 USD. No request names the market, the token or the account alone. `after.json` equals `before.json` in orders and collateral.
- **IF the placement gets no answer, THEN** the `pending` line stays. The test prints it and stops. The request is not sent again. Read the venue with `writes_the_polymarket_reads`, then run the kill switch.
- **Check at the venue:** the order history of the account shows one buy of 5 shares at 0.01, cancelled.
- **Kill switch:** `cancels_only_the_orders_of_the_polymarket_tests`. **Rollback:** IF the buy filled, THEN `sells_back_the_shares_of_the_held_placement` sells the shares back.

The test refuses each of these in code, before it sends an order:

- [ ] credentials that the venue did not derive, before and after the start gate
- [ ] a geoblock flag that does not read `blocked = false`
- [ ] a token other than the token of the tests, or a window with 90 seconds or less left
- [ ] a minimum size of the venue above 5 shares
- [ ] an order above 5 shares, or above the ceiling of 6 USD
- [ ] an exposure above 6 USD
- [ ] a buy of the tests that already rests on the token
- [ ] a buy that takes the collateral below 65 USD

## `cancel_finishes_while_a_marketable_place_is_held`

This test is written, and no run of this job starts it or its dry run. The live policy permits one signed order test at Polymarket and one order for each side. This test is a second signed order test, and it holds two buys at one time. So its live form refuses in code before it sends an order.

- **Dry run:** `cancel_finishes_while_a_marketable_place_is_held_dry_run` builds the resting buy, the marketable buy and the cancel, and prints them redacted. It sends no order action.
- **What it does, when a policy permits it:** it places one resting post-only buy of 5 shares at 0.01. Then it sends a marketable buy of 5 shares at the best ask on the placement lane of the engine, and at the same time the cancel of the resting buy on the control lane. It prints which answer came first, which says whether a cancel finishes while a marketable placement is held.
- **Kill switch:** `cancels_only_the_orders_of_the_polymarket_tests`. **Rollback:** `sells_back_the_shares_of_the_held_placement`.

## `cancels_only_the_orders_of_the_polymarket_tests`

Only `live-runner` runs this test, by the runbook of its job.

- **Dry run first:** `cancels_only_the_orders_of_the_polymarket_tests_dry_run`. It reads the id file and prints each cancel that it would send. It sends no cancel.
- **What it does:** it reads the id file. It sends one cancel for each venue order id of the file, one at a time. It reads the open orders of the token, and prints each order that it left and each `pending` line.
- **Expected outcome:** each order of the tests is cancelled. Each other order rests. No request names the market, the token or the account alone.
- **Check at the venue:** the open orders of the account hold no order of the tests.
- **Rollback:** `sells_back_the_shares_of_the_held_placement`, when a fill bought shares.

CAUTION: This test never cancels each order of the account. An engine never touches an order that it did not place. IF the test prints an order that it left, THEN do not cancel that order. Report it.

## `sells_back_the_shares_of_the_held_placement`

Only `live-runner` runs this test, by the runbook of its job.

- **When:** run it when `after.json` shows more shares of the token than `before.json`.
- **Dry run first:** `sells_back_the_shares_of_the_held_placement_dry_run`. It reads the account and builds the sell. Then it prints the sell redacted. It sends nothing.
- **What it does:** it reads `before.json` and the shares now. It sends one sell of the shares above `before`, at most 5, at the best bid, for one match. Its `pending` line, then its venue order id, go to the id file. IF the shares above `before` are below the minimum of the venue, THEN it sends nothing and prints why.
- **Expected outcome:** the shares equal `before.json`. The loss is at most the spread on 5 shares.
- **Check at the venue:** the order history shows one sell of the tests.
- **Kill switch:** `cancels_only_the_orders_of_the_polymarket_tests`.

The test refuses each of these in code: credentials that the venue did not derive, a geoblock flag that does not read `blocked = false`, a sell above 5 shares or above the ceiling of 6 USD, and a sell of the tests that already rests on the token.

## `silent_user_stream_still_answers_reads`

Only `live-runner` runs this test, by the runbook of its job. It uses real funds on live Binance spot, BTCUSDT, under the caps and the tag of the tests of the Binance sections above.

- **Dry run first:** `silent_user_stream_still_answers_reads_dry_run`. It probes the key and reads the venue. Then it builds the buy, the read of the open orders and the cancel, and prints each request with `apiKey` and `signature` as `<redacted>`. It sends no order action and writes no file.
- **What it does:**
  1. It opens the order session of the WebSocket API and subscribes to the user data stream.
  2. It places one post-only buy of about 10 USDT, 2 % below the best bid, under the tag of the tests.
  3. The venue answers `NEW`, and the stream sends one execution report `NEW`.
  4. It waits 35 seconds, and counts the execution reports of the stream in that time: 0 is expected.
  5. It reads the open orders on the same session. The answer names the buy.
  6. It cancels the buy by its client order id. The stream sends the report `CANCELED`.
  7. It prints whether a read still answers while the stream is silent: YES when the read of step 5 answered inside 10 seconds while step 4 saw no report, ELSE NO.
- **Expected outcome:** the buy is cancelled by its client order id, with no fill. Each request, answer and stream event in `silence/` of the run folder is redacted at write, and no balance event is written.
- **Check at the venue:** the order history shows one buy of the tests, cancelled.
- **Kill switch:** `cancels_only_the_orders_of_the_tests`. **Rollback:** `sells_back_the_base_above_before`, IF the buy filled.

The test refuses each of these in code, before it sends an order:

- [ ] a symbol other than BTCUSDT
- [ ] a buy above 10 USDT, or a ceiling above 25 USDT
- [ ] an exposure above 50 USDT
- [ ] a buy of the tests that already rests
- [ ] a buy below the minimum of the venue
- [ ] a buy that takes the free USDT below 20 USDT
