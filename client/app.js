/*
 * LiveRecall browser client.
 *
 * Local model, not the DOM, is the source of truth:
 *   segments: Map<segmentNumber, entry>
 * Every inbound message -- live CAPTION_UPDATE or batched HISTORY item --
 * goes through the single idempotent merge path, so deduplication,
 * revision replacement and reconnect recovery all share one implementation.
 */

const WS_URL = "ws://127.0.0.1:8765";

const statusEl = document.getElementById("status");
const liveEl = document.getElementById("live");
const liveEmptyEl = document.getElementById("live-empty");
const recallEl = document.getElementById("recall");
const jumpEl = document.getElementById("jump");
const copyEl = document.getElementById("copy");

const recallTitleEl = document.getElementById("recall-title");
const recallSubEl = document.getElementById("recall-sub");
const statSegmentsEl = document.getElementById("stat-segments");
const statFinalEl = document.getElementById("stat-final");
const statResumeEl = document.getElementById("stat-resume");

let ws;

let reconnectAttempt = 0;
let reconnectTimer = null;
let closedByUs = false;
let statusHoldTimer = null;

const RECALL_RETAIN_SECONDS = 120;   // >= 60 s required; headroom for the 60 s view
const DEFAULT_RECALL_SECONDS = 30;
const MAX_HISTORY_SECONDS = 60;      // protocol maximum we may request on resume
let recallSeconds = DEFAULT_RECALL_SECONDS;

const MAX_LIVE_ROWS = 60;
const AUTOSCROLL_SLACK_PX = 40;

const RECONNECT_BASE_MS = 500;
const RECONNECT_MAX_MS = 5000;

const segments = new Map();

// Latest *contiguous* finalized state held locally. Sent on CLIENT_HELLO.
let lastFinalSegment = 0;
let lastFinalRevision = 0;

let lastRecallItems = [];

/* ------------------------------------------------------------------ *
 * Merge
 * ------------------------------------------------------------------ */

function mergeCaption(msg, quiet = false) {
  const seg = Number(msg.segment);
  const rev = Number(msg.revision);
  if (!Number.isFinite(seg) || !Number.isFinite(rev)) return false;

  const existing = segments.get(seg);

  // Stale or duplicate revision.
  if (existing && rev <= existing.revision) return false;

  // A final caption is durable state: never let a later provisional downgrade it.
  if (existing && existing.final && !msg.final) return false;

  let ts = Number(msg.source_ts);
  if (!Number.isFinite(ts)) ts = nowSeconds();   // never let a bad ts hide a caption

  const entry = existing || { segment: seg, el: null };
  entry.revision = rev;
  entry.text = String(msg.text ?? "");
  entry.final = Boolean(msg.final);
  entry.source_ts = ts;
  segments.set(seg, entry);

  if (entry.final) advanceResumeWatermark();

  renderSegment(entry, quiet);
  updateStats();
  return true;
}

/*
 * Advance only across a gapless run of finalized segments. Reporting the
 * highest final we happen to hold would tell the server we already have a
 * segment we actually missed, and it would never be resent.
 */
function advanceResumeWatermark() {
  let next = segments.get(lastFinalSegment + 1);
  while (next && next.final) {
    lastFinalSegment = next.segment;
    lastFinalRevision = next.revision;
    next = segments.get(lastFinalSegment + 1);
  }
}

function updateStats() {
  if (!statResumeEl) return;
  let finals = 0;
  for (const entry of segments.values()) if (entry.final) finals++;
  if (statSegmentsEl) statSegmentsEl.textContent = String(segments.size);
  if (statFinalEl) statFinalEl.textContent = String(finals);
  statResumeEl.textContent = `segment ${lastFinalSegment} rev ${lastFinalRevision}`;
}

/* ------------------------------------------------------------------ *
 * Live rendering
 * ------------------------------------------------------------------ */

function isNearBottom() {
  return liveEl.scrollHeight - liveEl.scrollTop - liveEl.clientHeight < AUTOSCROLL_SLACK_PX;
}

function renderSegment(entry, quiet = false) {
  // Capture scroll intent BEFORE mutating the DOM.
  const stick = isNearBottom();

  if (liveEmptyEl && liveEmptyEl.isConnected) liveEmptyEl.remove();

  if (!entry.el) {
    entry.el = document.createElement("div");
    entry.el.className = "caption";
    entry.el.dataset.segment = String(entry.segment);
    insertInSegmentOrder(entry.el, entry.segment);
  }

  entry.el.classList.toggle("final", entry.final);
  entry.el.classList.toggle("provisional", !entry.final);

  const seg = document.createElement("span");
  seg.className = "seg";
  seg.textContent = String(entry.segment);

  const text = document.createElement("span");
  text.className = "text";
  text.textContent = entry.text;

  const badge = document.createElement("span");
  badge.className = "badge";
  badge.textContent = entry.final ? "final" : `rev ${entry.revision}`;
  text.append(badge);

  entry.el.replaceChildren(seg, text);

  // Skip the attention flash while draining a history batch.
  if (!quiet) {
    entry.el.classList.remove("flash");
    void entry.el.offsetWidth;   // force reflow so the animation restarts
    entry.el.classList.add("flash");
  }

  trimLiveRows();
  if (stick) liveEl.scrollTop = liveEl.scrollHeight;
  updateJumpPill();
}

function insertInSegmentOrder(el, segment) {
  let before = null;
  for (const child of liveEl.children) {
    const n = Number(child.dataset.segment);
    if (Number.isFinite(n) && n > segment) { before = child; break; }
  }
  liveEl.insertBefore(el, before);
}

function trimLiveRows() {
  while (liveEl.children.length > MAX_LIVE_ROWS) {
    const first = liveEl.firstElementChild;
    const entry = segments.get(Number(first.dataset.segment));
    if (entry) entry.el = null;   // entry stays in the store; only the node goes
    first.remove();
  }
}

function updateJumpPill() {
  if (!jumpEl) return;
  jumpEl.classList.toggle("show", !isNearBottom() && liveEl.children.length > 0);
}

liveEl.addEventListener("scroll", updateJumpPill);
jumpEl?.addEventListener("click", () => {
  liveEl.scrollTop = liveEl.scrollHeight;
  updateJumpPill();
});

/* ------------------------------------------------------------------ *
 * Local store bounds
 * ------------------------------------------------------------------ */

function nowSeconds() {
  return Date.now() / 1000;
}

function pruneStore() {
  const cutoff = nowSeconds() - RECALL_RETAIN_SECONDS;
  for (const [segment, entry] of segments) {
    if (entry.source_ts < cutoff) {
      entry.el?.remove();
      segments.delete(segment);
    }
  }
}

/* ------------------------------------------------------------------ *
 * Recall view -- answered entirely from local state
 * ------------------------------------------------------------------ */

function renderRecall() {
  pruneStore();
  const cutoff = nowSeconds() - recallSeconds;

  const items = [...segments.values()]
    .filter((entry) => entry.final && entry.source_ts >= cutoff)
    .sort((a, b) => a.segment - b.segment);

  lastRecallItems = items;
  if (copyEl) copyEl.disabled = items.length === 0;

  if (recallTitleEl) recallTitleEl.textContent = "What did I miss?";

  if (items.length === 0) {
    if (recallSubEl) {
      recallSubEl.textContent =
        `The last ${recallSeconds} seconds, answered from captions already on this device.`;
    }
    recallEl.innerHTML = `<p class="empty">No finalized captions in the last ${recallSeconds} seconds.</p>`;
    return;
  }

  const span = Math.max(1, Math.round(nowSeconds() - items[0].source_ts));
  if (recallSubEl) {
    recallSubEl.textContent =
      `${items.length} caption${items.length === 1 ? "" : "s"} from the last ` +
      `${recallSeconds} seconds (${span} s of lecture), from local state.`;
  }

  recallEl.replaceChildren(...items.map((entry) => {
    const row = document.createElement("div");
    row.className = "recall-line";
    const age = document.createElement("span");
    age.className = "age";
    age.textContent = `-${Math.round(nowSeconds() - entry.source_ts)}s`;
    const text = document.createElement("span");
    text.textContent = entry.text;
    row.append(age, text);
    return row;
  }));
}

setInterval(renderRecall, 1000);

copyEl?.addEventListener("click", async () => {
  if (lastRecallItems.length === 0) return;
  const blob = lastRecallItems.map((e) => e.text).join("\n");
  try {
    await navigator.clipboard.writeText(blob);
    const original = copyEl.textContent;
    copyEl.textContent = "Copied";
    setTimeout(() => { copyEl.textContent = original; }, 1500);
  } catch {
    copyEl.textContent = "Copy failed";
    setTimeout(() => { copyEl.textContent = "Copy to notes"; }, 1500);
  }
});

/* ------------------------------------------------------------------ *
 * Status
 * ------------------------------------------------------------------ */

function setStatus(text, cls, holdMs = 0) {
  clearTimeout(statusHoldTimer);
  statusEl.textContent = text;
  statusEl.className = cls;
  if (holdMs > 0) {
    statusHoldTimer = setTimeout(() => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        statusEl.textContent = "connected";
        statusEl.className = "ok";
      }
    }, holdMs);
  }
}

/* ------------------------------------------------------------------ *
 * Connection
 * ------------------------------------------------------------------ */

function numericClientId() {
  const key = "liverecall-client-id";
  let value = sessionStorage.getItem(key);
  if (!value) {
    value = String(Math.floor(100000 + Math.random() * 900000));
    sessionStorage.setItem(key, value);
  }
  return Number(value);
}

function connect() {
  clearTimeout(reconnectTimer);
  setStatus("connecting", "warn");
  ws = new WebSocket(WS_URL);

  ws.addEventListener("open", () => {
    const resuming = lastFinalSegment > 0;
    reconnectAttempt = 0;
    setStatus("connected", "ok");
    ws.send(JSON.stringify({
      type: "CLIENT_HELLO",
      client_id: numericClientId(),
      session: "L1",
      last_segment: lastFinalSegment,      // latest contiguous FINALIZED state
      last_revision: lastFinalRevision,
      history_seconds: MAX_HISTORY_SECONDS // cover the widest recall window we offer
    }));
    ws.__resuming = resuming;
  });

  ws.addEventListener("message", (event) => {
    let msg;
    try { msg = JSON.parse(event.data); } catch { return; }

    if (msg.type === "CAPTION_UPDATE") {
      mergeCaption(msg);
      return;
    }

    if (msg.type === "HISTORY") {
      const items = Array.isArray(msg.items) ? msg.items : [];
      const ordered = items.slice().sort((a, b) => Number(a.segment) - Number(b.segment));

      let merged = 0;
      for (const item of ordered) if (mergeCaption(item, true)) merged++;

      liveEl.scrollTop = liveEl.scrollHeight;
      renderRecall();
      console.log(
        `HISTORY ${items.length} item(s) received, ${merged} merged, ` +
        `${items.length - merged} already held`
      );

      if (merged > 0) {
        const what = ws.__resuming ? "recovered" : "loaded";
        setStatus(`connected - ${what} ${merged} caption${merged === 1 ? "" : "s"} you missed`,
                  "recovered", 6000);
      }
    }
  });

  ws.addEventListener("close", () => {
    if (closedByUs) return;
    scheduleReconnect();
  });

  ws.addEventListener("error", () => {
    setStatus("connection error", "bad");
  });
}

function scheduleReconnect() {
  reconnectAttempt++;
  const backoff = Math.min(RECONNECT_BASE_MS * (2 ** (reconnectAttempt - 1)), RECONNECT_MAX_MS);
  // Jitter: 100 simultaneously-dropped clients must not retry in lockstep.
  const delay = backoff * (0.5 + Math.random() * 0.5);
  setStatus(`disconnected, retrying in ${(delay / 1000).toFixed(1)} s`, "bad");
  reconnectTimer = setTimeout(connect, delay);
}

/* ------------------------------------------------------------------ *
 * Controls
 * ------------------------------------------------------------------ */

for (const button of document.querySelectorAll("button[data-seconds]")) {
  button.addEventListener("click", () => {
    recallSeconds = Number(button.dataset.seconds);
    for (const other of document.querySelectorAll("button[data-seconds]")) {
      other.classList.toggle("active", other === button);
    }
    renderRecall();   // no network request: all windows are views over local state
  });
}

const dropButton = document.getElementById("drop");
dropButton?.addEventListener("click", () => {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  closedByUs = true;
  ws.close();
  setStatus("manually disconnected, reconnecting in 5 s", "bad");
  setTimeout(() => {
    closedByUs = false;
    connect();
  }, 5000);
});

window.addEventListener("beforeunload", () => {
  closedByUs = true;
  if (ws) ws.close();
});

renderRecall();
connect();
