#!/usr/bin/env python3
"""
CS3103 Assignment 4 - LiveRecall naive working baseline.

This starter is intentionally useful but incomplete. It provides the required
CLI and a basic WebSocket broadcaster so that caption_source.py -> server ->
browser/load clients works on day one.

It DOES NOT implement the graded networking mechanisms:
  - revision-state management or deduplication
  - recent-history recovery or late-join recovery
  - bounded per-client application queues
  - slow-client isolation
  - semantic coalescing / backpressure
  - constrained-bandwidth overload handling

Required evaluator command:
    python solution_server.py --port 8765

Teams should replace or substantially extend the implementation while keeping
this command-line interface and the protocol in the assignment specification.
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

# Each client's live backlog has a hard limit. 
MAX_PENDING_MESSAGES = 512
MAX_DEFERRED_PROVISIONALS = 128

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
    pending: deque[dict[str, Any]] = field(default_factory=deque)
    deferred_provisionals: OrderedDict[int, dict[str, Any]] = field(
        default_factory=OrderedDict
    )
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    closing: bool = False

    def enqueue(self, message: dict[str, Any]) -> bool:
        """Queue a live update without allowing the queue to grow indefinitely.

        Returns False only when a final cannot fit because the bounded queue is
        already full of finals. The final remains in shared history; the caller
        closes that lagging connection so it can resume from history.
        """
        is_final = bool(message.get("final"))
        segment = int(message["segment"])

        if is_final:
            # A final supersedes any delayed provisional for this segment.
            self.deferred_provisionals.pop(segment, None)
            if len(self.pending) >= MAX_PENDING_MESSAGES:
                removed_provisional = False
                for index, queued in enumerate(self.pending):
                    if not queued.get("final", False):
                        del self.pending[index]
                        removed_provisional = True
                        break

                if not removed_provisional:
                    return False

            self.pending.append(message)
            self.ready.set()
            return True

        if len(self.pending) < MAX_PENDING_MESSAGES:
            self.pending.append(message)
            self.ready.set()
            return True

        for index, queued in enumerate(self.pending):
            if (
                not queued.get("final", False)
                and int(queued.get("segment", -1)) == segment
            ):
                self.pending[index] = message
                self.ready.set()
                return True

        for index, queued in enumerate(self.pending):
            if not queued.get("final", False):
                del self.pending[index]
                self.pending.append(message)
                self.ready.set()
                return True

        self.deferred_provisionals[segment] = message
        self.deferred_provisionals.move_to_end(segment)
        while len(self.deferred_provisionals) > MAX_DEFERRED_PROVISIONALS:
            self.deferred_provisionals.popitem(last=False)
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
    while True:
        await client.ready.wait()

        while client.pending or client.deferred_provisionals:
            if client.pending:
                message = client.pending.popleft()
            else:
                _, message = client.deferred_provisionals.popitem(last=False)

            try:
                await client.ws.send(json.dumps(message, separators=(",", ":")))
            except Exception:
                try:
                    await client.ws.close()
                except Exception:
                    pass
                return

        # No await occurs between checking both buffers and clearing the event,
        # so an enqueue cannot be lost here on the asyncio event loop.
        client.ready.clear()



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
        print(f"client disconnected: id={client_id}; clients={len(CLIENTS)}", flush=True)


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
    ):
        print(f"LiveRecall baseline listening on ws://0.0.0.0:{port}", flush=True)
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
