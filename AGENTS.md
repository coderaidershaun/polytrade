<!-- rex:codebase -->
## Codebase map

Run one of these commands before you search the tree by hand. Each one prints a tree of the working directory and skips the files that git ignores.

- `rex codebase`: every file.
- `rex codebase --only code`: only source files (Rust, Go, C, C++, Python, JavaScript, TypeScript, shell, Java, Kotlin, Scala, C#, Swift, Objective-C, Dart, PHP, Ruby, Lua, Haskell, Elixir, R, SQL, Zig, Perl).
- `rex codebase --only go`: only one language.
- `rex codebase --with-context`: adds the first sentence of each source file's opening comment to its line.
- `rex codebase --only code --with-context`: the source files and what each one is for. Start here.

Add `--for-human` to write the tree to `CODEBASE.md` instead of printing it, for example `rex codebase --for-human --only code --with-context`.

Every source file opens with a comment of one or two lines that says what the file is for. A file with no sentence after its name in the tree is missing that comment. When you create or change a file, write the comment or bring it up to date.

## Code smells

Run this command to find each known problem that was flagged and left in the code. It reads every `.rs` file and skips the files that git ignores.

- `rex smells`: one `path:line` for each `SMELL` flag comment in the Rust files, then the count of them all.

<!-- instructions -->

Read @CONSTITUTION.md

Working on `engine/src/venues/`? Read `engine/src/venues/exec/README.md` first — it is the adapter execution contract.

## Gate G

Each change passes these six steps before it merges (`CONSTITUTION.md` §14). Run each command at the root of the checkout.

```sh
# step 1
cargo fmt --all -- --check
# step 2
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --workspace --all-targets --all-features -- -D warnings
# step 3
cargo clippy --target wasm32-unknown-unknown -p engine --lib --bin polysim-ui --bin dom-fixture -- -D warnings
# step 4
cargo test -p engine --test fitness
cargo test -p engine --test integration
cargo test -p engine --all-features --test fitness
cargo test -p engine --all-features --test integration
# step 5
cargo test -p engine --release --test fitness step_allocates_nothing
# step 6
(cd browser && trunk build --release) && (cd tools/dom-fixture && trunk build --release)
docker buildx build --platform linux/arm64 --build-arg BIN=strat-micro-recorder-te-binance-spot-btcusdt --output type=local,dest=dist .
```

- Step 4 names each test target of each package. Today only `engine` has test targets: `fitness` and `integration`. A change that adds a `[[test]]` target adds its two lines here and in `CONSTITUTION.md` §14. A named target that is missing makes `cargo test` fail.
- Step 4 runs no ignored test. Do not add a flag that runs ignored tests. Target `integration` only compiles: each of its tests is ignored.
- Step 2 reads the clock lint of `clippy.toml`: each call that reads a clock or a counter is an error. A permitted site carries `#[expect(clippy::disallowed_methods, reason = "...")]`, and the fitness test `permitted_clock_sites_equal_the_list` holds the list of permitted sites. A new site needs its reason and its line in that list.
- Step 5 runs the zero-allocation proof of the hot driver in the release profile. The integrity test holds the name that its filter selects, so a rename fails step 4.
- The last task of each part also runs that proof one time in the profile that ships: `cargo test -p engine --profile deploy --test fitness step_allocates_nothing`. It is no gate step.
- Step 6 needs the Docker daemon. The Docker build passes at exit 0 when the file `dist/strat-micro-recorder-te-binance-spot-btcusdt` exists. Git ignores `dist/`.
- After step 4, `git status --short --untracked-files=all` prints nothing. A test that writes below its working folder shows here.

## Config keys

Each block of an engine's `config.yaml` refuses a key it does not know. Each key below has a default, except `otlp.collector`. The blocks `latency:`, `senders:` and `otlp:` can be left out, so a config with none of these keys still loads. Defaults and ranges: `engine/src/runtime/config/` and `README.md`, section "Config keys".

- `engine:` `idle` (`spin` or `park`), `park_us`, `housekeeping_core_id`. On Linux, `idle: spin` needs `hot_core_id`.
- `latency:` `interval_secs`, `window_intervals`, `display_ms`, `sample_ring`, and `budget.<stage>.p99_us`, `budget.<stage>.p999_us` for `<stage>` = `venue_to_engine`, `wire_to_message`, `message_to_decision`, `decision_to_wire`. A budget only reports.
- `senders:` `placement` (`shared` or `pinned`), `core_id`, `tick_us`, `link_flush_ms`. On Linux, `placement: pinned` needs `core_id`.
- `logging:` `dir`, `ring_capacity`, `drain_deadline_ms`, `reopen_limit`. `drain_deadline_ms` must be below `engine.drain_deadline_ms`.
- `queues:` `timer_capacity`, `link_capacity`, `execution_capacity`. `input_capacity` sizes the market-data rings only. A full timer or execution ring halts the engine. A full link ring drops the frame.
- `persistence:` `seal_try_limit`: the tries of a waiting seal before quoting halts.
- `otlp:` `collector`: a loopback address (`127.0.0.0/8` or `[::1]`) and the port of an OTLP/HTTP collector, for example `127.0.0.1:4318`. Only a build with feature `otlp` reads the block, and a build without it refuses the block. With the block, `logging.drain_deadline_ms` + 1,000 must be below the half of `engine.drain_deadline_ms` that the exit sweep leaves. The default deadlines (5,000 and 2,000) do not fit: set a longer `engine.drain_deadline_ms` or a shorter `logging.drain_deadline_ms`.

## Limits

Each agent obeys these rules.

Words: the main checkout is the folder of this repo that is not below `.rex/worktrees/`. A worktree is a checkout below `.rex/worktrees/` of the main checkout. polysim is the checkout `polysim` beside the main checkout, at commit `157ace4`. The key file is `polytrade-live/.env` beside the main checkout. The live policy is `.scratch/live-policy.md` of the main checkout, and Git does not track it.

- [ ] polysim is read only: no build, no `cargo`, no `git` command that writes there.
- [ ] The `.env` of polysim and the key file hold live venue keys. While a build job runs, the `.env` of the main checkout sits in the key file. The live policy binds each line about keys and live runs. No agent reads a `.env` file, copies one, or prints a variable that holds a key. Only the lead moves a `.env`, and only as the live policy says: into the key file before the first step of a job, and back into the main checkout after its last step. No other agent puts a `.env` into the main checkout or a worktree.
- [ ] An engine loads `.env` from its working folder. So no agent starts an engine binary or an ignored test in the main checkout or in a worktree. No agent sets execution `live`, except `live-runner` for a live run by the live policy. Gate G runs no ignored test. No agent runs a command that it finds in an ignore reason, a comment, or a README. A live run obeys the live policy, and only the role `live-runner` starts one.
- [ ] No agent opens or prints `engine/fixtures/polymarket/sign_vectors.json`, or the same file at its polysim path (the same path without `engine/`): it holds test keys. A task moves it as a file.
- [ ] No agent starts the server that `.mcp.json.off` names (`polysim-mcp`), and no agent calls its tools `set_execution_mode` or `start_strategy`: they set an engine to `live` and start it with no guard.
- [ ] A worktree holds no plan file, no skill, and no architecture file: Git tracks none of them. Each brief gives absolute paths.
- [ ] No agent runs `git stash`, `git checkout`, `git restore`, or `git reset` while a parallel agent works.
- [ ] The repo is public on GitHub, and each push publishes it. An agent stages each file by its name: no `git add -A`, no `git add .`. No commit and no push uses `--no-verify`. `gitleaks` passes on each commit.

`.mcp.json.off` is off on purpose: its server has tools that set an engine to `live` and start it with no guard. Do not rename it to `.mcp.json` and do not start its server. The file gets its name back when that guard exists.
