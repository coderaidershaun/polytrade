# Runbook: tape-sync

This runbook tells a person how to install, start, check and stop the job that ships the tape files of an engine host to a second folder. The job runs the binary `tape-sync` (`tools/tape-sync/`) on a systemd timer. The law of the tape is in `CONSTITUTION.md` §6 and §7 (sections of 2026-11-19).

## What the job does

- The job reads each run folder below the source folder: `{persistence.dir}/{strategy-id}/{te-id}/{kind}/{run_id}/`.
- The job ships each sealed file (`*.parquet`) of each closed run. A closed run is a run whose `run.json` gives an end state, or a run that a crash cut. A cut run has a `run.json` that says `open`, but its engine does not hold its identity lock, or a newer run of the same engine and kind exists.
- For each file, the job copies the file under a temporary name (`.copying`), syncs it, renames it, and syncs its folder. Then it compares the checksum of the copy with the checksum of the local file. Only when the two are equal does the job delete the local file.
- The job ships `run.json` last, when no other file of the run is left, and then removes the empty folders of the run.
- The job never reads, copies or deletes a `.part` file. It never touches a file of an open run.
- The job reads no key, no `.env` file and no network. It reads only the source folder, the destination folder and the identity locks of the engines.

Exit codes:

| Code | Meaning | Action |
| --- | --- | --- |
| 0 | Each file of each closed run shipped, or was rightly left. | None. |
| 1 | The job refused one file or one run, at least. It kept each refused local file. | Read the journal (step 9). |
| 2 | The arguments are wrong, or the job cannot list the source folder. | Correct the environment file (step 4). |

## Before you start

1. Make sure that the host runs Linux with systemd, and that the engines run on this host. The job reads the identity lock of each engine, and that lock is a file in the temporary folder of this host.
2. Find the user that runs the engines. This runbook calls it `polytrade`. The job must run as this user, because it deletes shipped files below the folder of that user.
3. Find the `persistence.dir` of each engine config (`strategies/<strategy-id>/<te-id>/config.yaml`). This folder is the source. The shipped configs give `./data`, which is relative to the working folder of the engine: use its absolute path. IF the engines of the host use two folders, THEN install a second pair of units, with other names and a second environment file.
4. Choose the destination: a folder on another disk, or on a mounted file system of another host.

CAUTION: Use a destination whose file system makes a sync durable. The job deletes the local file after it syncs the copy. A file system that confirms a sync that it did not do can lose the only copy of a file.

CAUTION: Set `persistence.retention_hours` of each engine config well above the longest run of that engine, for example 48 for a run of 24 hours. Retention deletes the old sealed files of each run, an open run too. The job ships no file of an open run. So a run that lasts longer than its retention loses its oldest files before they ship.

## Install

1. Build the binary for the host. On the build machine, at the root of the repository, run:

   ```sh
   docker buildx build --platform linux/arm64 --build-arg BIN=tape-sync --output type=local,dest=dist .
   ```

   The file `dist/tape-sync` is the binary. On a host that has the toolchain of `rust-toolchain.toml`, you can build it there:

   ```sh
   cargo build --locked --profile deploy -p engine --bin tape-sync
   ```

   Then the binary is `target/deploy/tape-sync`.

2. Copy the binary to the host, and install it:

   ```sh
   sudo install -m 0755 dist/tape-sync /usr/local/bin/tape-sync
   ```

3. Install the two unit files of this folder:

   ```sh
   sudo install -m 0644 ops/tape-sync/tape-sync.service ops/tape-sync/tape-sync.timer /etc/systemd/system/
   ```

4. Write the environment file `/etc/default/tape-sync`, with the two folders of "Before you start":

   ```sh
   sudo tee /etc/default/tape-sync <<'EOF'
   TAPE_SYNC_SOURCE=/srv/polytrade/data
   TAPE_SYNC_DESTINATION=/mnt/tape-archive
   EOF
   ```

   Replace the two example paths with the real folders.

5. IF the engines do not run as `polytrade`, THEN set their user in a drop-in. Run this command, and in the editor write `[Service]`, then `User=<user>` and `Group=<group>`:

   ```sh
   sudo systemctl edit tape-sync.service
   ```

CAUTION: Do not add `PrivateTmp=yes` to the service. A private temporary folder hides the identity lock of each engine. Then the job reads each open run as cut, and it ships the sealed files of runs that are still running.

## First run, by hand

6. Run the job one time as the user of the engines, and read its last line:

   ```sh
   sudo -u polytrade /usr/local/bin/tape-sync --source /srv/polytrade/data --destination /mnt/tape-archive
   echo "exit $?"
   ```

   The last line of a good run is `tape-sync: shipped <n> files, left <m> open runs, refused 0`, and the exit code is 0. Each running engine adds one open run.

7. Make sure that the destination holds the shipped files below the same folders, for example:

   ```sh
   find /mnt/tape-archive -name 'run.json' | head
   ```

## Start the timer

8. Load the units and start the timer:

   ```sh
   sudo systemctl daemon-reload
   sudo systemctl enable --now tape-sync.timer
   ```

   The timer starts the job 5 minutes after boot, and then 15 minutes after the end of each run of the job.

## Check

9. Read the state of the timer and of the last run:

   ```sh
   systemctl list-timers tape-sync.timer
   systemctl status tape-sync.service
   journalctl -u tape-sync.service --since today
   ```

   IF the last run ended with exit 1, THEN the journal names each refused file or run, with each cause under it:

   | Line in the journal | Cause | Action |
   | --- | --- | --- |
   | `kept the local file:` and a copy error | The copy failed: the destination is full, or the user cannot write there. | Correct the destination. The next run ships the file. |
   | `kept the local file:` and a checksum error | A different file with the same name is at the destination. The job kept both files. | Compare the two files. Remove the wrong one by hand. |
   | `left <run folder>:` | The `run.json` of the run is missing or does not parse, or the job cannot probe the lock. The job left the whole run. | Read the `run.json` of that run. |

   IF the last run ended with exit 2, THEN correct `/etc/default/tape-sync` (step 4), and run step 6 again.

CAUTION: Do not delete a local tape file by hand while its copy is not proven. The job deletes a file only after the checksums are equal, and a deleted file cannot come back.

10. A crash between a copy and its rename can leave one `.copying` file at the destination. The next copy of the same file writes over it. Remove such a file only while no run of the job is active:

    ```sh
    systemctl is-active tape-sync.service
    find /mnt/tape-archive -name '*.copying'
    ```

    The first command must print `inactive` before you remove a file.

## Stop

11. Stop the timer, and keep the installed files:

    ```sh
    sudo systemctl disable --now tape-sync.timer
    ```

    A run of the job that is active ends by itself. The local files that it did not ship stay where they are.
