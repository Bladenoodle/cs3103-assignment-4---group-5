#!/usr/bin/env python3
"""
CS3103 Assignment 4 - LiveRecall server.

Required evaluator command:
    python solution_server.py --port 8765

Design summary
--------------
* Revision state: accept_caption() keeps the newest revision per active
  segment and never lets an older revision, or anything after a final,
  replace newer state.
* Bounded history: FINAL_HISTORY keeps finalized captions for 60 s (and at
  most MAX_HISTORY_ITEMS); HISTORY replies are built from it on CLIENT_HELLO
  for reconnect / late join, excluding finals the client already holds.
* Per-client isolation: every client has its own bounded semantic queue
  (ClientState.pending) and its own writer task (send_live_updates), so a
  slow client never delays the others.
* Overload policy (semantic backpressure):
    1. The queue holds at most one message per segment - the newest state -
       so obsolete provisionals are replaced, never queued behind each other.
    2. Application-level flow control: at most MAX_INFLIGHT messages may be
       unconfirmed (ping/pong) per client, so backlog stays in the queue where
       it can be coalesced instead of in TCP/kernel buffers.
    3. A client that hits backpressure enters congested mode (finals only),
       with exponential back-off of the hold time; it returns to full updates
       once it keeps up.
    4. Messages are sent as fragmented WebSocket messages so that receiver-side
       frame buffering hides fewer messages and backpressure is seen early.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Any

import websockets

# The server retains at most the most recent minute 
HISTORY_SECONDS = 60.0
MAX_HISTORY_ITEMS = 10_000
MAX_TRACKED_SEGMENTS = 1_024

# Per-client overload policy (see ClientState and send_live_updates).
#
# MAX_PENDING_SEGMENTS bounds the application-level queue. Because the queue
# holds at most ONE message per segment (the newest state), this is a bound on
# how many segments a client may lag behind, not on how many revisions.
MAX_PENDING_SEGMENTS = 256
#
# MAX_INFLIGHT bounds how many messages may be "in the network" for a client
# (sent by us but not yet confirmed as read by the client's WebSocket library).
# Without this, a slow reader's backlog silently accumulates in kernel TCP
# buffers and the transport write buffer, where it can no longer be coalesced.
MAX_INFLIGHT = 2
#
# Small transport write buffer, for the same reason as MAX_INFLIGHT.
WRITE_LIMIT_BYTES = 4096
#
# A client whose in-flight window filled up recently is "congested": it is
# sent finals only, while its newest provisional stays coalesced in its queue
# (and is superseded by the final). This lets the backlog hidden in the
# client's receive buffers drain instead of being refilled with revisions
# that will be obsolete by the time they are read.
#
# The hold time backs off exponentially: a client that becomes congested again
# soon after leaving congested mode is evidently still slow, so its hold is
# doubled (up to the max). A client that stays healthy for a long time gets
# its hold reset, so a recovered client returns to full-update behaviour.
CONGESTION_HOLD_MIN_S = 5.0
CONGESTION_HOLD_MAX_S = 60.0
#
# Receiver-side buffering is invisible to the server: a typical WebSocket
# client library keeps reading frames into its own queue (16 frames by
# default in the `websockets` library) before it stops reading from TCP.
# Those frames are "already delivered" from TCP's point of view, so no
# backpressure is visible until that queue is full - up to 16 stale messages.
# For a client that has already shown it is slow, we send each message as a
# fragmented WebSocket message (same JSON text, split over several frames,
# reassembled transparently by any RFC 6455 client). The receiver's frame
# queue then holds far fewer messages, backpressure becomes visible much
# earlier, and probing whether the client has recovered becomes cheap.
# Clients that have never been congested use DEFAULT_FRAGMENTS (below).
SLOW_CLIENT_FRAGMENTS = 8
#
# Every client starts with light fragmentation so that a slow reader is
# detected after ~4 buffered messages instead of ~16, before it is known to be
# slow. Measured trade-off (public T2/T3, see report): normal-client P50
# +1-8 ms (100-400 clients) vs slow-client P95 8.1 s -> 1.5 s.
DEFAULT_FRAGMENTS = 4

@dataclass
class HistoryEntry:
    received_at: float
    session: str
    message: dict[str, Any]

@dataclass(eq=False)
class ClientState:
    ws: Any
    client_id: Any
    session: str
    # segment -> newest not-yet-sent state of that segment, in segment order.
    pending: OrderedDict[int, dict[str, Any]] = field(default_factory=OrderedDict)
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    closing: bool = False
    # Congested (finals-only) mode lasts until congested_until.
    congested_until: float = 0.0
    hold_s: float = CONGESTION_HOLD_MIN_S
    congestion_events: int = 0
    # True once this client has ever been congested (see SLOW_CLIENT_FRAGMENTS).
    suspect_slow: bool = False
    # Statistics, useful for the report / viva.
    coalesced: int = 0

    def congested(self, now: float) -> bool:
        return now < self.congested_until

    def mark_congested(self, now: float) -> None:
        """Called when the in-flight window is full (real backpressure)."""
        if not self.congested(now):
            if self.congested_until and now - self.congested_until < self.hold_s:
                # Re-congested soon after recovering: still slow, back off.
                self.hold_s = min(self.hold_s * 2, CONGESTION_HOLD_MAX_S)
            elif self.congested_until and now - self.congested_until > 2 * self.hold_s:
                self.hold_s = CONGESTION_HOLD_MIN_S
            self.congestion_events += 1
            self.suspect_slow = True
        self.congested_until = max(self.congested_until, now + self.hold_s)

    def pop_next(self, now: float) -> dict[str, Any] | None:
        """Choose the next message to send, or None to hold back."""
        if not self.pending:
            return None
        segment, message = next(iter(self.pending.items()))
        if message.get("final", False) or not self.congested(now):
            del self.pending[segment]
            return message
        # Congested and the head is a provisional. Because the source finishes
        # a segment before starting the next, a provisional at the head means
        # no final is waiting behind it except for later segments.
        for seg, queued in self.pending.items():
            if queued.get("final", False):
                # Should not happen with in-order segments, but never let a
                # held provisional block a final.
                del self.pending[seg]
                return queued
        return None

    def enqueue(self, message: dict[str, Any]) -> bool:
        """Semantic, bounded per-client queue.

        Invariant: the queue holds at most one message per segment, and it is
        always the newest accepted state of that segment.

        * Fast client: the writer sends each update before the next one
          arrives, so nothing is ever replaced -> full revision coverage.
        * Slow client / constrained link: an update arrives while an older,
          still-unsent revision of the same segment is queued. The older one
          is obsolete, so it is replaced in place (keeping its position, so
          segments are still delivered in order). A final always replaces a
          provisional and is never replaced or dropped.

        Returns False only when the client lags by more than
        MAX_PENDING_SEGMENTS segments; the caller then closes the connection
        and the client resumes from the bounded server history.
        """
        segment = int(message["segment"])
        queued = self.pending.get(segment)

        if queued is not None:
            if queued.get("final", False):
                return True  # final is durable; never overwrite it
            self.pending[segment] = message  # replace obsolete provisional
            self.coalesced += 1
            self.ready.set()
            return True

        if len(self.pending) >= MAX_PENDING_SEGMENTS:
            # Prefer discarding the oldest queued provisional over failing.
            for seg, queued in self.pending.items():
                if not queued.get("final", False):
                    del self.pending[seg]
                    self.coalesced += 1
                    break
            else:
                return False

        self.pending[segment] = message
        self.ready.set()
        return True


CLIENTS: set[ClientState] = set()
FINAL_HISTORY: deque[HistoryEntry] = deque()
LATEST_BY_SEGMENT: OrderedDict[tuple[str, int], dict[str, Any]] = OrderedDict()
FINALIZED_THROUGH: dict[str, int] = {}
STATE_LOCK = asyncio.Lock()

def prune_history(now: float) -> None:
    while FINAL_HISTORY and now - FINAL_HISTORY[0].received_at > HISTORY_SECONDS:
        FINAL_HISTORY.popleft()
    while len(FINAL_HISTORY) > MAX_HISTORY_ITEMS:
        FINAL_HISTORY.popleft()


def positive_int(value: Any) -> int | None:
    # bool is a subclass of int, but it isn't a valid segment/revision number.
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


def nonnegative_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return default
    return value


def requested_seconds(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 30.0
    return max(0.0, min(float(value), HISTORY_SECONDS))

async def close_lagging_client(client: ClientState) -> None:
    try:
        await client.ws.close(
            code=1013,
            reason="Client fell behind; reconnect to resume recent captions",
        )
    except Exception:
        pass

def make_history(session: str, hello: dict[str, Any], now: float) -> list[dict[str, Any]]:
    seconds = requested_seconds(hello.get("history_seconds", 30))
    last_segment = nonnegative_int(hello.get("last_segment", 0))
    last_revision = nonnegative_int(hello.get("last_revision", 0))
    cutoff = now - seconds

    items = []
    for entry in FINAL_HISTORY:
        if entry.session != session or entry.received_at < cutoff:
            continue

        message = entry.message
        segment = int(message["segment"])
        revision = int(message["revision"])
        if segment < last_segment:
            continue
        if segment == last_segment and revision <= last_revision:
            continue
        items.append(message)

    items.sort(key=lambda item: (int(item["segment"]), int(item["revision"])))
    return items

async def broadcast(message: dict[str, Any]) -> None:
    lagging: list[ClientState] = []
    for client in tuple(CLIENTS):
        if client.session != str(message.get("session", "L1")):
            continue
        if not client.enqueue(message) and not client.closing:
            client.closing = True
            lagging.append(client)

    for client in lagging:
        asyncio.create_task(close_lagging_client(client))


async def accept_caption(message: dict[str, Any], source_session: str) -> None:
    segment = positive_int(message.get("segment"))
    revision = positive_int(message.get("revision"))
    text = message.get("text")
    is_final = message.get("final")

    if segment is None or revision is None or not isinstance(text, str):
        return
    if not isinstance(is_final, bool):
        return

    session = str(message.get("session", source_session))
    if session != source_session:
        return

    # Preserve any extra source fields for forward compatibility, while
    # normalizing the required fields and preventing malformed data entering
    # shared state.
    accepted = dict(message)
    accepted.update(
        {
            "type": "CAPTION_UPDATE",
            "session": session,
            "segment": segment,
            "revision": revision,
            "final": is_final,
            "text": text,
        }
    )

    key = (session, segment)
    async with STATE_LOCK:
        if segment <= FINALIZED_THROUGH.get(session, 0):
            # Segment numbers are monotonic in the required source protocol.
            # Once a segment at or below this watermark is finalized, no later
            # message for it can be a valid new state.
            return
        previous = LATEST_BY_SEGMENT.get(key)
        if previous is not None:
            if previous.get("final", False):
                return
            if revision <= int(previous["revision"]):
                return

        LATEST_BY_SEGMENT[key] = accepted
        LATEST_BY_SEGMENT.move_to_end(key)
        now = time.monotonic()
        if is_final:
            FINAL_HISTORY.append(HistoryEntry(now, session, accepted))
            prune_history(now)
            FINALIZED_THROUGH[session] = max(
                FINALIZED_THROUGH.get(session, 0), segment
            )
            # Final captions live in the bounded history, so retaining a
            # second per-segment copy is unnecessary. Drop finalized state up
            # through the watermark; it remains protected from replay above.
            for old_key in tuple(LATEST_BY_SEGMENT):
                old_session, old_segment = old_key
                if (
                    old_session == session
                    and old_segment <= FINALIZED_THROUGH[session]
                ):
                    del LATEST_BY_SEGMENT[old_key]
        while len(LATEST_BY_SEGMENT) > MAX_TRACKED_SEGMENTS:
            LATEST_BY_SEGMENT.popitem(last=False)

        await broadcast(accepted)


async def handle_source(ws: Any, hello: dict[str, Any]) -> None:
    session = str(hello.get("session", "L1"))
    print(f"source connected: session={session}", flush=True)

    try:
        async for raw in ws:
            try:
                message = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(message, dict):
                continue

            message_type = message.get("type")
            if message_type == "CAPTION_UPDATE":
                await accept_caption(message, session)
            elif message_type == "SOURCE_DONE":
                print("source finished", flush=True)
    finally:
        print("source disconnected", flush=True)

async def send_live_updates(client: ClientState) -> None:
    """Per-client writer with application-level flow control.

    After every data message we send a WebSocket ping. The client's library
    answers with a pong only once it has *read* everything before the ping,
    so an outstanding ping means "data still sitting in the network".
    We allow at most MAX_INFLIGHT outstanding pings. When a client reads
    slowly, the writer therefore stops early and new updates wait in
    client.pending - the one place where obsolete provisionals can be
    coalesced - instead of in kernel buffers where they would go stale.
    Fast clients answer pings within milliseconds and are never held back.
    """
    inflight: deque[Any] = deque()
    try:
        while True:
            await client.ready.wait()
            while client.pending:
                while inflight and inflight[0].done():
                    inflight.popleft()
                now = time.monotonic()
                if len(inflight) >= MAX_INFLIGHT:
                    # Backpressure: the client is not reading fast enough.
                    client.mark_congested(now)
                    await asyncio.wait({inflight[0]}, timeout=1.0)
                    continue

                message = client.pop_next(now)
                if message is None:
                    # Holding a provisional for a congested client: wait for
                    # a new update (e.g. its final) or the hold to expire.
                    client.ready.clear()
                    waits = [asyncio.ensure_future(client.ready.wait())]
                    if inflight:
                        waits.append(inflight[0])
                    await asyncio.wait(waits, timeout=0.25,
                                       return_when=asyncio.FIRST_COMPLETED)
                    if not waits[0].done():
                        waits[0].cancel()
                    continue

                payload = json.dumps(message, separators=(",", ":"))
                frags = SLOW_CLIENT_FRAGMENTS if client.suspect_slow else DEFAULT_FRAGMENTS
                if frags > 1:
                    step = max(1, -(-len(payload) // frags))
                    await client.ws.send(
                        [payload[i:i + step] for i in range(0, len(payload), step)]
                    )
                else:
                    await client.ws.send(payload)
                inflight.append(await client.ws.ping())

            # No await occurs between checking pending and clearing the event,
            # so an enqueue cannot be lost here on the asyncio event loop.
            client.ready.clear()
    except Exception:
        try:
            await client.ws.close()
        except Exception:
            pass
    finally:
        for waiter in inflight:
            if not waiter.done():
                waiter.cancel()


async def handle_client(ws: Any, hello: dict[str, Any]) -> None:
    client_id = hello.get("client_id")
    session = str(hello.get("session", "L1"))
    now = time.monotonic()

    async with STATE_LOCK:
        prune_history(now)
        history_items = make_history(session, hello, now)
        client = ClientState(ws=ws, client_id=client_id, session=session)
        # Register before sending HISTORY. Any live updates arriving while the
        # HISTORY send is in progress are queued and sent afterward.
        CLIENTS.add(client)

    writer: asyncio.Task[None] | None = None
    try:
        await ws.send(
            json.dumps(
                {"type": "HISTORY", "items": history_items},
                separators=(",", ":"),
            )
        )
        writer = asyncio.create_task(send_live_updates(client))
        print(f"client connected: id={client_id}; clients={len(CLIENTS)}", flush=True)
        async for _ in ws:
            pass
    finally:
        CLIENTS.discard(client)
        if writer is not None:
            writer.cancel()
            try:
                await writer
            except asyncio.CancelledError:
                pass
        print(
            f"client disconnected: id={client_id}; clients={len(CLIENTS)}; "
            f"coalesced={client.coalesced}; congestion_events={client.congestion_events}",
            flush=True,
        )


async def handler(ws: Any) -> None:
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=5)
        hello = json.loads(raw)
    except (asyncio.TimeoutError, json.JSONDecodeError, TypeError):
        await ws.close(code=1008, reason="Expected a valid hello message")
        return

    if not isinstance(hello, dict):
        await ws.close(code=1008, reason="Expected a JSON object hello message")
        return

    message_type = hello.get("type")
    if message_type == "SOURCE_HELLO":
        await handle_source(ws, hello)
    elif message_type == "CLIENT_HELLO":
        await handle_client(ws, hello)
    else:
        await ws.close(code=1008, reason="Expected SOURCE_HELLO or CLIENT_HELLO")


async def main_async(port: int) -> None:
    async with websockets.serve(
        handler,
        "0.0.0.0",
        port,
        max_size=2**20,
        ping_interval=20,
        ping_timeout=20,
        write_limit=WRITE_LIMIT_BYTES,
    ):
        print(f"LiveRecall server listening on ws://0.0.0.0:{port}", flush=True)
        await asyncio.Future()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    try:
        asyncio.run(main_async(args.port))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
