# LiveRecall - CS3103 Assignment 4 (Group 5)

A WebSocket caption relay: `caption_source.py` -> `solution_server.py` -> browser client / simulated load clients.
The server keeps the newest revision of every segment, never loses a final caption, isolates slow clients,
recovers reconnecting and late-joining clients from a bounded 60 s history, and adapts when the
network is impaired (delay, loss, or a bandwidth cap).

## 1. Environment (tested)

- Ubuntu 24.04.1 LTS (the Ubuntu VM from the Mininet lab, run inside a VM), Python 3.12.3
- `tc` / `netem` from `iproute2` (used on the loopback interface `lo` for the impairment tests)
- Only Python dependency: `websockets>=13,<17` (tested with 16.1.1)

## 2. Setup

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip iproute2
git clone <repo-url> && cd cs3103-assignment-4---group-5
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python submission_check.py          # structure + startup check
```

Every command below assumes the virtual environment is active (`source venv/bin/activate`),
so `python` is the venv's Python 3.12.

## 3. Start the server

```bash
python solution_server.py --port 8765
```

The server listens on `ws://0.0.0.0:8765`. Source and clients both connect to this address.
Log lines show connections, and each disconnect prints per-client statistics
(`coalesced`, `congestion_events`, `path_rtt_ms`, `lean`). A line
`link monitor: lean mode ON/OFF` is printed when the bandwidth-limit mode switches.

## 4. Browser client demo

Use three terminals, all in the repo directory with the venv active.

```bash
# Terminal 1 - server
python solution_server.py --port 8765

# Terminal 2 - serve the browser client
python -m http.server 8000 --directory client

# Terminal 3 - caption source (long run so the 60 s recall view is meaningful)
python caption_source.py --uri ws://127.0.0.1:8765 --segments 120
```

Open `http://127.0.0.1:8000` in at least two browser tabs. The client shows the live caption,
replaces a segment's text when a newer revision arrives (it never shows an older revision after a newer
one), and offers a recall view of the last 15 / 30 / 60 seconds. If the connection drops it reconnects
with backoff and resumes from the last segment/revision it holds. To try recovery, stop and restart the
server or close and reopen a tab while the source is running. See `DEMO_GUIDE.md` for the demo script.

## 5. Public deterministic tests

Each command starts the server, the simulated clients and the caption source itself
(`run_demo.py`), writes `<stem>.csv` / `<stem>.json`, and prints the metrics. Use `--runs 3` for
the reported median of three independent runs.

| Test | Command |
|------|---------|
| T1 baseline | `python run_demo.py --clients 20 --duration 20 --runs 3 --output-stem t1` |
| T2 100 clients | `python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem t2` |
| T3 slow clients (10 % read 500 ms late) | `python run_demo.py --clients 100 --duration 60 --runs 3 --slow-percent 10 --slow-ms 500 --output-stem t3` |
| T4 disconnect and recover (20 % for 5 s at t=15 s) | `python run_demo.py --clients 100 --duration 60 --runs 3 --disconnect-percent 20 --disconnect-at 15 --disconnect-seconds 5 --output-stem t4` |

### T5 network impairment (Linux `tc`, needs `sudo`)

The profiles in `network_profiles/` act on the loopback interface. Always reset afterwards.

```bash
bash network_profiles/delay_100ms.sh     # or loss_3pct.sh, delay_loss.sh, reorder.sh
python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem t5_delay100
bash network_profiles/reset.sh
tc qdisc show dev lo                     # must print "noqueue"
```

### Constrained bandwidth

```bash
bash network_profiles/bandwidth_2mbit.sh
python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem bw_2mbit
bash network_profiles/reset.sh

# our own tighter cap (not a supplied profile)
sudo tc qdisc add dev lo root tbf rate 500kbit burst 16kbit latency 400ms
python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem bw_500kbit
bash network_profiles/reset.sh
```

### Reproduce all results (before vs after)

`scripts/run_all.sh` runs every test above and writes summaries to `results/`.

```bash
sudo -v                        # cache the sudo password first
scripts/run_all.sh before      # naive starter baseline (built from the original commit)
scripts/run_all.sh after       # our solution
python3 scripts/summarize.py   # medians over the runs
```

Outputs: `results/<test>_before.txt`, `results/<test>_after.txt`, `results/env_*.txt`
(machine description), raw logs in `results/raw/` (not committed).
The harness occasionally crashes with `load_clients.py failed` under heavy impairment; the script
retries a run up to five times and `summarize.py` reports the number of failed attempts.
Run the tests on an otherwise idle VM, with at least 1 GB of free disk.

## 6. Design summary

- **Revision state**: per segment the newest revision wins; nothing replaces a final or arrives
  after it. Stale or duplicate revisions are dropped.
- **Bounded history**: finals are kept for 60 s (max 10 000). `HISTORY` is sent on `CLIENT_HELLO`,
  excluding what the client already holds, which handles reconnect and late join.
- **Per-client queue and writer**: each client has its own queue with at most one message per
  segment (the newest state) and its own writer task. A slow client cannot delay the others.
- **Semantic backpressure**: obsolete provisionals are coalesced; finals are never dropped. A
  ping/pong window measures how far behind a client is. A truly slow reader is switched to
  finals-only until it recovers, a long (high-RTT) path is not mistaken for a slow reader.
- **Bandwidth-limit (lean) mode**: if most clients are slow at the same moment, the shared link
  is the bottleneck. The server then sends unfragmented messages and far fewer pings, which
  removes protocol overhead on a nearly full link, while keeping queues, coalescing and finals
  priority. It switches back automatically when pongs are fast again.

## 7. Known limitations

- Under a 500 kbit cap, delivery of every provisional revision is about 92 %; the missing ones are
  deliberately coalesced, and every final caption is delivered.
- The public harness can crash with `load_clients.py failed` under heavy delay; this is a harness
  issue, so affected runs are retried.

## 8. Repository layout

```
solution_server.py        server (entry point: python solution_server.py --port 8765)
client/                   browser client (index.html, app.js)
scripts/                  run_all.sh, summarize.py (reproduce before/after results)
network_profiles/         supplied tc/netem profiles (+ reset.sh)
results/                  before/after summaries per test and environment notes
caption_source.py, load_clients.py, run_demo.py, analyse_results.py, submission_check.py   supplied tools
README.md, AI_USAGE.md, CONTRIBUTIONS.md, report.pdf
```
