# AI / Coding-Assistant Usage

## Tools used

| Tool | Used for |
|---|---|
| Claude (Anthropic), conversational | Design review of `solution_server.py`; proposing and critiquing changes to the revision-state, recovery, and backpressure logic; interpreting public-test output and server logs; diagnosing a client-side crash observed during T3. | |

No AI tool was given write access to the repository. Every change was applied
by a team member, reviewed line by line, and accepted only after it was
re-measured with the supplied public test harness (`run_demo.py`,
`analyse_results.py`) without modification to the measurement logic.

Our working rule was that an AI suggestion counted as evidence of a
*hypothesis*, never of a result. Two of the suggestions we received were
confidently argued and wrong, and the public tests are what caught them; both
are documented below.

## One useful suggestion

**Suggestion: replace the server-wide RTT estimate with a per-client one.**

Our earlier design kept a single process-wide estimate of path RTT and derived
every client's in-flight window from it:

```python
class PathRtt:
    def update(self, sample):
        if sample < self.value:
            self.value = 0.5 * self.value + 0.5 * sample   # fast down
        else:
            self.value += 0.01 * (sample - self.value)     # slow up

    def window(self):
        return min(MAX_WINDOW, MIN_WINDOW + int(self.value / WINDOW_STEP_S))
```

The assistant pointed out that because the estimator moves downward with
weight 0.5 and upward with weight 0.01, a single fast loopback client pins
`value` near its own RTT. Every other client then inherits
`window() == MIN_WINDOW == 2`, including a client sitting behind hundreds of
milliseconds of delay — which would be limited to two messages per RTT
regardless of available bandwidth. Since the hidden evaluator explicitly
combines network delay with a slow-reader population, that mixed case is
certain to occur.

We replaced it with per-client state. Each `ClientState` now keeps `rtt_min`
(the minimum pong latency observed on that connection) and its own `window`,
grown additively on each healthy pong and reset on congestion:

```python
def note_pong(self, latency, now):
    if self.rtt_min is None or latency < self.rtt_min:
        self.rtt_min = latency
    if latency <= self.healthy_latency():
        if self.pending and self.window < MAX_WINDOW:
            self.window += 1
```

A *minimum* is the right statistic here because a client that reads slowly
inflates its pong latencies but can never lower them, so it cannot raise its
own congestion threshold and hide from detection.

**How we verified it.** Server logs now print each client's final `window` and
`rtt_min_ms` on disconnect. On T1 and T2 every client converges to
`window=24` with `congestion_events=0`, confirming the window is no longer
pinned low. On T3 the slow population is the only group that stays at
`window=2` with `congestion_events >= 1`, confirming the two cases are being
distinguished per client rather than globally. T1 and T2 hold 100% final
completeness and 100% Normal Update Coverage, so the change costs nothing
under normal conditions.

## One suggestion that was wrong, inefficient, or required modification

We record two, because the second was uncovered by acting on the first and
together they changed how we treat generated reasoning.

### (a) "Default message fragmentation is overhead buying nothing — remove it"

The server splits each outgoing WebSocket message into several frames for
low-RTT clients. The stated purpose is that a receiving library buffers a
bounded number of *frames*, so fragmenting makes that buffer fill after fewer
messages and exposes backpressure to the server sooner.

The assistant argued this should be removed for ordinary clients:

> `DEFAULT_FRAGMENTS = 4` quadruples frame count and send-path work for every
> client on a low-RTT link, to make a receiver-side buffer fill sooner. The
> 16-frame figure is an assumption about the client library. I'd set the
> default to 1 and fragment only for clients that have shown congestion.

The reasoning is plausible — the frame-buffer size genuinely is an assumption
about `load_clients.py` — and the CPU cost at 100+ clients is real. We treated
it as a hypothesis and made it the single variable in a before/after T3 run.

**Result — the suggestion was wrong, and dangerously so:**

| T3 (100 clients, 10% slow @ 500 ms) | `DEFAULT_FRAGMENTS = 1` | `DEFAULT_FRAGMENTS = 4` |
|---|---|---|
| Final-caption completeness | **98.42%** (below the 99% gate) | **99.87%** |
| Slow-client P50 caption age | 12685 ms | 2158 ms |
| Slow-client P95 caption age | 19369 ms | 5102 ms |
| Overall P95 caption age | 2252 ms | 1774 ms |

The server log explained the mechanism. With fragmentation disabled, slow
clients still showed `congestion_events=1` but only `coalesced=26` — so
congestion was being *detected*, yet almost nothing was being coalesced. The
backlog was not in our `pending` queue at all; it was sitting in the client
library's internal receive queue, downstream of anything the server can
observe or coalesce. Fragmentation is what forces that queue to reach its
limit sooner, which is what makes the backlog visible to us in time to act
on it.

**Correction.** We restored `DEFAULT_FRAGMENTS = 4` and additionally measured
8 and 16 to check we were not simply at an untested point: 8 improves
slow-client P95 slightly (3308 ms) but degrades normal-client P95 from 17.8 ms
to 23.6 ms, and 16 is worse on both axes. The original value was at the
measured optimum.

### (b) "Lower `CONGEST_MIN_AGE_S` from 0.5 s to 0.25 s to close the remaining 0.13%"

With fragmentation restored, T3 completeness sat at 99.87% — above the gate,
but not perfect. The assistant suggested detecting congestion sooner, so that
fewer provisional revisions are buffered in a slow client before the overload
policy engages.

We applied the change and reran T3. The run **crashed**, inside the
instructor's `load_clients.py`:

```
File ".../websockets/asyncio/messages.py", line 60, in reset
    assert not self.queue, "cannot reset() while queue isn't empty"
AssertionError: cannot reset() while queue isn't empty
```

**Diagnosis.** `load_clients.py` reads with
`asyncio.wait_for(ws.recv(), timeout=timeout)`. If that timeout fires while a
*fragmented* message is only partly reassembled, the cancelled `recv()` leaves
frames in the library's assembler and the next `recv()` trips the assertion.
Lowering the threshold did not introduce the bug; it increased exposure to it,
because more clients flip to `suspect_slow` sooner and escalate from 4
fragments to `SLOW_CLIENT_FRAGMENTS = 8`, widening the window in which a
timeout can land mid-message.

This is the hazard already noted in our module docstring — that partially
delivered fragmented messages are fragile for receivers that cancel a read —
and we had under-weighted it.

**Correction and the judgement behind it.** We reverted
`CONGEST_MIN_AGE_S` to 0.5 s and reduced `SLOW_CLIENT_FRAGMENTS` from 8 to 4,
removing the escalation entirely: the 8-fragment path was measurably worse for
normal clients and doubled our exposure to this race for no completeness
benefit. We then re-ran T3 to confirm the configuration is both stable and
still above the gate.

The wider lesson shaped the rest of our work. The hidden evaluator runs this
same `load_clients.py`, so a configuration that crashes one run in ten is
strictly worse than one that is marginally less complete and never crashes —
chasing the last 0.13% of completeness was the wrong objective. Fragmentation
is a deliberate trade we keep because the measurements in (a) show the 99%
gate depends on it, but we keep it at the lowest setting that clears the gate
rather than the setting that optimises the metric.

## Responsibility statement

We understand that the team remains responsible for the submitted code,
experimental results, and claims, and that every member may be asked to
explain or modify relevant code during the viva.

Specifically, we accept that AI involvement does not transfer responsibility
for any of the following, each of which we can explain and defend: the
per-segment finality rule and why it is not a monotonic watermark; the
per-client window and why `rtt_min` is a minimum rather than an average; the
ordering rule in `pop_next` that gives finals strict priority while preserving
the active segment's latest revision; the placement of `broadcast()` inside
`STATE_LOCK` and the duplicate-final race that occurs if it is moved outside;
and the fragmentation trade-off documented above, including its failure mode.
