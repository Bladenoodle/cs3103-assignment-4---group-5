/*
 * Working but deliberately naive LiveRecall browser starter.
 *
 * Provided mechanical behavior:
 *   - opens a WebSocket
 *   - sends CLIENT_HELLO
 *   - visibly appends every received CAPTION_UPDATE
 *   - displays whether an update is provisional/final
 *
 * Graded TODOs:
 *   - replace revisions rather than appending duplicates
 *   - maintain >=60 s of finalized local state
 *   - implement 30 s default recall + 15/30/60 s views
 *   - automatic reconnect + correct resume state
 *   - HISTORY merge/deduplication
 *   - usable final UI
 */

const WS_URL = "ws://127.0.0.1:8765";
const statusEl = document.getElementById("status");
const liveEl = document.getElementById("live");
const recallEl = document.getElementById("recall");

const recallTitleEl = document.getElementById("recall-title");
const statSegmentsEl = document.getElementById("stat-segments");
const statFinalEl = document.getElementById("stat-final");
const statResumeEl = document.getElementById("stat-resume");

let ws;

let reconnectAttempt = 0;
let reconnectTimer = null;
let closedByUs = false;

const RECALL_RETAIN_SECONDS = 120;
const DEFAULT_RECALL_SECONDS = 30;
let recallSeconds = DEFAULT_RECALL_SECONDS;

const MAX_LIVE_ROWS = 60;

const RECONNECT_BASE_MS = 500;
const RECONNECT_MAX_MS = 5000;

const segments = new Map();

let lastFinalSegment = 0;
let lastFinalRevision = 0;

function mergeCaption(msg) {
    const seg = Number(msg.segment);
    const rev = Number(msg.revision);
    const existing = segments.get(seg);

    if (existing && rev <= existing.revision) {
        // Ignore older or duplicate revisions.
        return false;
    }

    const entry = existing || { segment: seg, el: null };
    entry.revision = rev;
    entry.text = String(msg.text ?? "");
    entry.final = Boolean(msg.final);
    entry.source_ts = Number(msg.source_ts);
    segments.set(seg, entry);
    
    if (entry.final) {
      if (seg > lastFinalSegment ||
          (seg === lastFinalSegment && rev > lastFinalRevision)) {
        lastFinalSegment = seg;
        lastFinalRevision = rev;
      }
    }

    renderSegment(entry);
    
    updateStats();
    return true;
}

function updateStats() {
    if (!statResumeEl) return;
    let finals = 0;
    for (const entry of segments.values()) if (entry.final) finals++;
    statSegmentsEl.textContent = String(segments.size);
    statFinalEl.textContent = String(finals);
    statResumeEl.textContent = `segment ${lastFinalSegment} rev ${lastFinalRevision}`;
}

function renderSegment(entry) {
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
    seg.textContent = `#${entry.segment}`;

    const badge = document.createElement("span");
    badge.className = "badge";
    badge.textContent = entry.final ? "final" : `rev ${entry.revision}`;
    
    const text = document.createElement("span");
    text.className = "text";
    text.textContent = entry.text;

    entry.el.replaceChildren(seg, badge, text);

    entry.el.classList.remove("flash");
    void entry.el.offsetWidth; // force reflow
    entry.el.classList.add("flash");

    trimLiveRows();
    liveEl.scrollTop = liveEl.scrollHeight;
}

function insertInSegmentOrder(el, segment) {
  let before = null;
  for (const child of liveEl.children) {
    if (Number(child.dataset.segment) > segment) {
      before = child;
      break;
    }
  }
  liveEl.insertBefore(el, before);
}

function numericClientId() {
  const key = "liverecall-client-id";
  let value = sessionStorage.getItem(key);
  if (!value) {
    value = String(Math.floor(100000 + Math.random() * 900000));
    sessionStorage.setItem(key, value);
  }
  return Number(value);
}

function appendNaively(msg) {
  const row = document.createElement("div");
  row.className = "naive-line";
  const state = msg.final ? "FINAL" : "PROVISIONAL";
  row.textContent = `[${state}] segment ${msg.segment} rev ${msg.revision}: ${msg.text}`;
  liveEl.appendChild(row);
  liveEl.scrollTop = liveEl.scrollHeight;
}

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

function trimLiveRows() {
  while (liveEl.children.length > MAX_LIVE_ROWS) {
    const first = liveEl.firstElementChild;
    const entry = segments.get(Number(first.dataset.segment));
    if (entry) entry.el = null;
    first.remove();
  }
}

function renderRecall() {
  pruneStore();
  const cutoff = nowSeconds() - recallSeconds;

  const items = [...segments.values()]
    .filter((entry) => entry.final && entry.source_ts >= cutoff)
    .sort((a, b) => a.segment - b.segment);

  if (recallTitleEl) {
    recallTitleEl.textContent = `What did I miss in the last ${recallSeconds} seconds.`;
  }

  if (items.length === 0) {
    recallEl.textContent = `No finalized captions in the last ${recallSeconds} seconds.`;
    return;
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

function connect() {
  clearTimeout(reconnectTimer);
  statusEl.textContent = "connecting";
  ws = new WebSocket(WS_URL);

  ws.addEventListener("open", () => {
    reconnectAttempt = 0;
    statusEl.textContent = "connected";
    ws.send(JSON.stringify({
      type: "CLIENT_HELLO",
      client_id: numericClientId(),
      session: "L1",
      last_segment: lastFinalSegment,       // TODO: resume latest finalized state
      last_revision: lastFinalRevision,
      history_seconds: 30
    }));
  });

  ws.addEventListener("message", (event) => {
    const msg = JSON.parse(event.data);
    if (msg.type === "CAPTION_UPDATE") {
      mergeCaption(msg);
    } else if (msg.type === "HISTORY") {
      const items = Array.isArray(msg.items) ? msg.items : [];
      const ordered = items.slice().sort((a, b) => Number(a.segment) - Number(b.segment));

      let merged = 0;
      for (const item of ordered) {
        if (mergeCaption(item)) merged++;
      }
      console.log(`HISTORY ${items.length} item(s) received, ${merged} merged, ` + 
        `${items.length - merged} already held`);
      // TODO: merge history without stale/duplicate final captions.
    }
  });

  ws.addEventListener("close", () => {
    if (closedByUs) return;
    scheduleReconnect();
    // TODO: automatic reconnect after a short delay, using finalized resume state.
  });

  ws.addEventListener("error", () => {
    statusEl.textContent = "connection error";
  });
}

function scheduleReconnect() {
  reconnectAttempt++;
  const backoff = Math.min(RECONNECT_BASE_MS * (2 ** (reconnectAttempt - 1)), RECONNECT_MAX_MS);
  const delay = backoff * (0.5 + Math.random() * 0.5); // 50 - 100% of backoff
  statusEl.textContent = `disconnected, retrying in ${(delay / 1000).toFixed(1)} s`;
  reconnectTimer = setTimeout(connect, delay);
}

const dropButton = document.getElementById("drop");
if (dropButton) {
  dropButton.addEventListener("click", () => {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    closedByUs = true;
    ws.close();
    statusEl.textContent = "manually disconnected, reconnecting in 5 s";
    setTimeout(() => {
      closedByUs = false;
      connect();
    }, 5000);
  });
}

window.addEventListener("beforeunload", () => {
  closedByUs = true;
  if (ws) ws.close();
});

for (const button of document.querySelectorAll("button[data-seconds]")) {
  button.addEventListener("click", () => {
    recallSeconds = Number(button.dataset.seconds);
    for (const other of document.querySelectorAll("button[data-seconds]")) {
      other.classList.toggle("active", other === button);
    }
    renderRecall();
  });
}

renderRecall();

connect();
