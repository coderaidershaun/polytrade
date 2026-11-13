# Integration tests: runbook sections

Each test of this folder is ignored. The signed tests of `binance_recording.rs` use real funds on live Binance spot, BTCUSDT.

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
