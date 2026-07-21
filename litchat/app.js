/**
 * Litchat — Anonymous Stranger Chat (Omegle-style)
 * Single-file monolith: Express + Socket.io server + embedded frontend.
 * No external DB — all state lives in memory, safe for a single Docker container.
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = process.env.PORT || 3000;

// ---------------------------------------------------------------------------
// In-memory state
// ---------------------------------------------------------------------------

/** All currently connected sockets (for the live "total online" counter) */
let totalOnline = 0;

/**
 * Waiting queue entries: { socketId, tags: string[], joinedAt: number, fallbackTimer: Timeout }
 */
const waitingQueue = [];

/** socketId -> partnerSocketId, for everyone currently paired in a chat */
const activePairs = new Map();

/** socketId -> string[] tags, remembered per-connection so we can re-match after skip */
const userTags = new Map();

const FALLBACK_MATCH_MS = 5000;

function broadcastUserCount() {
  io.emit('user-count', totalOnline);
}

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function removeFromQueue(socketId) {
  const idx = waitingQueue.findIndex((entry) => entry.socketId === socketId);
  if (idx !== -1) {
    clearTimeout(waitingQueue[idx].fallbackTimer);
    waitingQueue.splice(idx, 1);
  }
}

function sharesTag(tagsA, tagsB) {
  if (!tagsA.length || !tagsB.length) return false;
  const setB = new Set(tagsB);
  return tagsA.some((t) => setB.has(t));
}

/** Attempt to find a queued partner that shares at least one tag with `tags`. */
function findTagMatch(excludeSocketId, tags) {
  return waitingQueue.find(
    (entry) => entry.socketId !== excludeSocketId && sharesTag(tags, entry.tags)
  );
}

/** Find literally any other queued user (fallback, ignores tags). */
function findAnyMatch(excludeSocketId) {
  return waitingQueue.find((entry) => entry.socketId !== excludeSocketId);
}

function pairSockets(socketA, socketB) {
  removeFromQueue(socketA.id);
  removeFromQueue(socketB.id);

  activePairs.set(socketA.id, socketB.id);
  activePairs.set(socketB.id, socketA.id);

  socketA.emit('matched');
  socketB.emit('matched');
}

/** Called when a user requests a match (initial find, or "Next"/skip). */
function enterMatchmaking(socket, tags) {
  userTags.set(socket.id, tags);

  // 1. Try an immediate tag-based match against everyone already waiting.
  const tagPartnerEntry = findTagMatch(socket.id, tags);
  if (tagPartnerEntry) {
    const partnerSocket = io.sockets.sockets.get(tagPartnerEntry.socketId);
    if (partnerSocket) {
      pairSockets(socket, partnerSocket);
      return;
    }
    // Stale entry (socket gone) — clean it up and continue.
    removeFromQueue(tagPartnerEntry.socketId);
  }

  // 2. No tag match right now — join the queue and let a 5s timer widen the search.
  socket.emit('searching');

  const entry = {
    socketId: socket.id,
    tags,
    joinedAt: Date.now(),
    fallbackTimer: null,
  };

  entry.fallbackTimer = setTimeout(function fallbackAttempt() {
    // Still waiting?
    const stillQueued = waitingQueue.some((e) => e.socketId === socket.id);
    if (!stillQueued) return;

    const anyPartnerEntry = findAnyMatch(socket.id);
    if (anyPartnerEntry) {
      const partnerSocket = io.sockets.sockets.get(anyPartnerEntry.socketId);
      if (partnerSocket) {
        pairSockets(socket, partnerSocket);
        return;
      }
      removeFromQueue(anyPartnerEntry.socketId);
    }

    // No one available yet — keep retrying every 5s until matched or cancelled.
    entry.fallbackTimer = setTimeout(fallbackAttempt, FALLBACK_MATCH_MS);
  }, FALLBACK_MATCH_MS);

  waitingQueue.push(entry);
}

/** Break a pairing (skip, disconnect) and notify the ex-partner. */
function unpair(socket, notifyPartner) {
  const partnerId = activePairs.get(socket.id);
  if (!partnerId) return;

  activePairs.delete(socket.id);
  activePairs.delete(partnerId);

  if (notifyPartner) {
    const partnerSocket = io.sockets.sockets.get(partnerId);
    if (partnerSocket) {
      partnerSocket.emit('partner-left');
    }
  }
}

// ---------------------------------------------------------------------------
// Socket.io wiring
// ---------------------------------------------------------------------------

io.on('connection', (socket) => {
  totalOnline += 1;
  broadcastUserCount();

  socket.on('find-stranger', (rawTags) => {
    // Always clear any prior state before entering matchmaking again.
    unpair(socket, true);
    removeFromQueue(socket.id);

    const tags = Array.isArray(rawTags)
      ? rawTags
          .map((t) => String(t).trim().toLowerCase())
          .filter(Boolean)
          .slice(0, 20)
      : [];

    enterMatchmaking(socket, tags);
  });

  socket.on('chat-message', (payload) => {
    const partnerId = activePairs.get(socket.id);
    if (!partnerId) return;
    const partnerSocket = io.sockets.sockets.get(partnerId);
    if (!partnerSocket) return;

    const rawText = payload && typeof payload === 'object' ? payload.text : payload;
    const rawId = payload && typeof payload === 'object' ? payload.id : null;
    const id = typeof rawId === 'string' ? rawId.slice(0, 64) : null;

    const clean = escapeHtml(rawText).slice(0, 2000);
    if (!clean.trim() || !id) return;

    partnerSocket.emit('chat-message', { id, text: clean });
    // Delivery ack: the partner's socket successfully received the event.
    socket.emit('message-delivered', { id });
  });

  socket.on('message-seen', (payload) => {
    const partnerId = activePairs.get(socket.id);
    if (!partnerId) return;
    const partnerSocket = io.sockets.sockets.get(partnerId);
    if (!partnerSocket) return;

    const id = payload && typeof payload.id === 'string' ? payload.id.slice(0, 64) : null;
    if (!id) return;

    partnerSocket.emit('message-seen', { id });
  });

  socket.on('typing', () => {
    const partnerId = activePairs.get(socket.id);
    if (!partnerId) return;
    const partnerSocket = io.sockets.sockets.get(partnerId);
    if (partnerSocket) partnerSocket.emit('typing');
  });

  socket.on('stop-typing', () => {
    const partnerId = activePairs.get(socket.id);
    if (!partnerId) return;
    const partnerSocket = io.sockets.sockets.get(partnerId);
    if (partnerSocket) partnerSocket.emit('stop-typing');
  });

  socket.on('skip', () => {
    unpair(socket, true);
    removeFromQueue(socket.id);
    const tags = userTags.get(socket.id) || [];
    enterMatchmaking(socket, tags);
  });

  socket.on('leave-chat', () => {
    unpair(socket, true);
    removeFromQueue(socket.id);
  });

  socket.on('disconnect', () => {
    totalOnline = Math.max(0, totalOnline - 1);
    unpair(socket, true);
    removeFromQueue(socket.id);
    userTags.delete(socket.id);
    broadcastUserCount();
  });
});

// ---------------------------------------------------------------------------
// Frontend (single self-contained HTML page)
// ---------------------------------------------------------------------------

app.get('/', (req, res) => {
  res.send(getHTML());
});

function getHTML() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>Litchat — Talk to Strangers</title>
<style>
  :root {
    --bg-base: #0f0c1b;
    --bg-deep: #1a103c;
    --sidebar-grad-start: #4c1d95;
    --sidebar-grad-end: #312e81;
    --accent: #8b5cf6;
    --accent-strong: #7c3aed;
    --accent-hover: #a78bfa;
    --bubble-you: #8b5cf6;
    --bubble-stranger: #33313f;
    --text-main: #f4f2ff;
    --text-dim: #b6aed6;
    --border-soft: rgba(255,255,255,0.08);
  }

  * { box-sizing: border-box; }

  html, body {
    margin: 0;
    padding: 0;
    height: 100%;
    font-family: 'Segoe UI', Roboto, -apple-system, BlinkMacSystemFont, sans-serif;
    background: linear-gradient(160deg, var(--bg-base), var(--bg-deep));
    color: var(--text-main);
    overflow: hidden;
    -webkit-tap-highlight-color: transparent;
    overscroll-behavior: none;
  }

  #app {
    display: flex;
    height: 100vh;
    height: 100dvh; /* real visible viewport on mobile browsers */
    width: 100vw;
  }

  /* ---------------- SIDEBAR ---------------- */
  #sidebar {
    width: 260px;
    min-width: 260px;
    background: linear-gradient(200deg, var(--sidebar-grad-start), var(--sidebar-grad-end));
    display: flex;
    flex-direction: column;
    padding: 20px 16px;
    padding-top: calc(20px + env(safe-area-inset-top));
    padding-left: calc(16px + env(safe-area-inset-left));
    border-right: 1px solid var(--border-soft);
  }

  #sidebarOverlay {
    display: none;
  }

  #closeSidebarBtn { display: none; }

  #menuToggleBtn { display: none; }

  .brand {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 24px;
  }

  .brand-logo {
    width: 34px;
    height: 34px;
    border-radius: 9px;
    background: rgba(255,255,255,0.15);
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 18px;
  }

  .brand-name {
    font-size: 20px;
    font-weight: 700;
    letter-spacing: 0.3px;
    flex-grow: 1;
  }

  #closeSidebarBtn {
    background: rgba(255,255,255,0.12);
    border: none;
    color: #fff;
    width: 30px;
    height: 30px;
    border-radius: 8px;
    font-size: 15px;
    cursor: pointer;
  }

  #menuToggleBtn {
    background: rgba(255,255,255,0.08);
    border: 1px solid rgba(255,255,255,0.12);
    color: var(--text-main);
    width: 38px;
    height: 38px;
    border-radius: 9px;
    font-size: 17px;
    cursor: pointer;
    flex-shrink: 0;
  }

  .side-block {
    background: rgba(255,255,255,0.07);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 10px;
    padding: 12px 14px;
    margin-bottom: 14px;
  }

  .side-block-label {
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    color: rgba(255,255,255,0.65);
    margin-bottom: 6px;
  }

  #userCount {
    font-size: 22px;
    font-weight: 700;
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .pulse-dot {
    width: 9px;
    height: 9px;
    border-radius: 50%;
    background: #4ade80;
    box-shadow: 0 0 0 0 rgba(74, 222, 128, 0.6);
    animation: pulse 1.8s infinite;
  }

  @keyframes pulse {
    0%   { box-shadow: 0 0 0 0 rgba(74, 222, 128, 0.55); }
    70%  { box-shadow: 0 0 0 8px rgba(74, 222, 128, 0); }
    100% { box-shadow: 0 0 0 0 rgba(74, 222, 128, 0); }
  }

  #statusLine {
    font-size: 13.5px;
    font-weight: 600;
    color: #fff;
  }

  #statusSub {
    font-size: 11.5px;
    color: rgba(255,255,255,0.6);
    margin-top: 3px;
  }

  .tags-wrap { flex-grow: 0; }

  #tagInput {
    width: 100%;
    padding: 9px 10px;
    border-radius: 8px;
    border: 1px solid rgba(255,255,255,0.15);
    background: rgba(0,0,0,0.2);
    color: #fff;
    font-size: 13px;
    outline: none;
  }
  #tagInput::placeholder { color: rgba(255,255,255,0.4); }
  #tagInput:focus { border-color: var(--accent-hover); }

  #tagChips {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-top: 10px;
  }

  .tag-chip {
    background: rgba(255,255,255,0.14);
    border: 1px solid rgba(255,255,255,0.18);
    padding: 3px 9px;
    border-radius: 999px;
    font-size: 11.5px;
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .tag-chip button {
    background: none;
    border: none;
    color: rgba(255,255,255,0.65);
    cursor: pointer;
    font-size: 12px;
    line-height: 1;
    padding: 0;
  }
  .tag-chip button:hover { color: #fff; }

  .sidebar-spacer { flex-grow: 1; }

  #findBtn {
    width: 100%;
    padding: 14px;
    border: none;
    border-radius: 10px;
    background: linear-gradient(135deg, var(--accent-strong), var(--accent));
    color: #fff;
    font-size: 15px;
    font-weight: 700;
    cursor: pointer;
    letter-spacing: 0.2px;
    transition: transform 0.12s ease, box-shadow 0.12s ease, filter 0.12s ease;
    box-shadow: 0 6px 18px rgba(124, 58, 237, 0.35);
  }
  #findBtn:hover { filter: brightness(1.08); transform: translateY(-1px); }
  #findBtn:active { transform: translateY(0); }
  #findBtn:disabled { opacity: 0.6; cursor: default; transform: none; }

  #findBtn.stop-mode {
    background: linear-gradient(135deg, #6d28d9, #4c1d95);
  }

  /* ---------------- MAIN PANE ---------------- */
  #main {
    flex-grow: 1;
    display: flex;
    flex-direction: column;
    background: radial-gradient(1200px 600px at 80% -10%, rgba(139,92,246,0.10), transparent),
                var(--bg-base);
    min-width: 0;
  }

  #chatHeader {
    height: 58px;
    min-height: 58px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 22px;
    border-bottom: 1px solid var(--border-soft);
    background: rgba(255,255,255,0.02);
  }

  #headerTitle {
    font-size: 15.5px;
    font-weight: 600;
    display: flex;
    align-items: center;
    gap: 9px;
  }

  #headerStatusDot {
    width: 9px;
    height: 9px;
    border-radius: 50%;
    background: #71717a;
  }
  #headerStatusDot.connected { background: #4ade80; }
  #headerStatusDot.searching { background: #fbbf24; animation: pulse 1.6s infinite; }

  #skipBtn {
    padding: 8px 16px;
    border-radius: 8px;
    border: 1px solid rgba(255,255,255,0.15);
    background: rgba(255,255,255,0.06);
    color: var(--text-main);
    font-size: 13px;
    font-weight: 600;
    cursor: pointer;
  }
  #skipBtn:hover { background: rgba(255,255,255,0.12); }
  #skipBtn:disabled { opacity: 0.4; cursor: default; }

  #messages {
    flex-grow: 1;
    overflow-y: auto;
    padding: 22px 26px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }

  #messages::-webkit-scrollbar { width: 8px; }
  #messages::-webkit-scrollbar-thumb { background: rgba(139,92,246,0.35); border-radius: 8px; }
  #messages::-webkit-scrollbar-track { background: transparent; }

  .msg-row { display: flex; }
  .msg-row.you { justify-content: flex-end; }
  .msg-row.stranger { justify-content: flex-start; }

  .bubble-wrap {
    max-width: 62%;
    display: flex;
    flex-direction: column;
  }
  .msg-row.you .bubble-wrap { align-items: flex-end; }
  .msg-row.stranger .bubble-wrap { align-items: flex-start; }

  .bubble {
    padding: 10px 14px;
    border-radius: 14px;
    font-size: 14.5px;
    line-height: 1.42;
    word-wrap: break-word;
    box-shadow: 0 2px 10px rgba(0,0,0,0.18);
  }

  .msg-row.you .bubble {
    background: linear-gradient(135deg, var(--bubble-you), #a78bfa);
    color: #fff;
    border-bottom-right-radius: 4px;
  }

  .msg-row.stranger .bubble {
    background: var(--bubble-stranger);
    color: #f1f0f6;
    border-bottom-left-radius: 4px;
  }

  .msg-status {
    display: flex;
    align-items: center;
    gap: 3px;
    margin-top: 3px;
    padding-right: 2px;
    font-size: 11px;
    color: var(--text-dim);
    height: 13px;
  }

  .msg-status svg { width: 14px; height: 14px; display: block; }
  .msg-status.seen svg path { stroke: #4ade80; }
  .msg-status .tick-sending {
    width: 8px; height: 8px; border-radius: 50%;
    border: 1.5px solid var(--text-dim);
    border-top-color: transparent;
    animation: spin 0.7s linear infinite;
  }
  @keyframes spin { to { transform: rotate(360deg); } }

  .msg-system {
    text-align: center;
    color: #9c96b8;
    font-size: 12.5px;
    font-style: italic;
    margin: 6px 0;
  }

  #typingIndicator {
    padding: 0 26px 6px;
    font-size: 12.5px;
    color: var(--text-dim);
    font-style: italic;
    min-height: 18px;
  }

  #inputBar {
    display: flex;
    gap: 10px;
    padding: 16px 22px 20px;
    border-top: 1px solid var(--border-soft);
    background: rgba(255,255,255,0.02);
  }

  #msgInput {
    flex-grow: 1;
    padding: 13px 16px;
    border-radius: 10px;
    border: 1px solid rgba(255,255,255,0.12);
    background: rgba(255,255,255,0.05);
    color: var(--text-main);
    font-size: 14.5px;
    outline: none;
  }
  #msgInput::placeholder { color: rgba(255,255,255,0.35); }
  #msgInput:focus { border-color: var(--accent-hover); }
  #msgInput:disabled { opacity: 0.5; }

  #sendBtn {
    padding: 0 26px;
    border: none;
    border-radius: 10px;
    background: linear-gradient(135deg, var(--accent-strong), var(--accent));
    color: #fff;
    font-weight: 700;
    font-size: 14.5px;
    cursor: pointer;
  }
  #sendBtn:hover { filter: brightness(1.08); }
  #sendBtn:disabled { opacity: 0.5; cursor: default; }

  .empty-state {
    margin: auto;
    text-align: center;
    color: var(--text-dim);
    max-width: 320px;
  }
  .empty-state .icon { font-size: 42px; margin-bottom: 12px; }
  .empty-state .title { font-size: 16px; font-weight: 700; color: var(--text-main); margin-bottom: 6px; }
  .empty-state .sub { font-size: 13px; line-height: 1.5; }

  /* =========================================================
     WELCOME SCREEN
     ========================================================= */
  #welcomeScreen {
    position: fixed;
    inset: 0;
    z-index: 100;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 24px;
    background:
      radial-gradient(900px 500px at 15% 0%, rgba(139,92,246,0.20), transparent),
      radial-gradient(900px 500px at 85% 100%, rgba(76,29,149,0.35), transparent),
      linear-gradient(160deg, var(--bg-base), var(--bg-deep));
  }

  #welcomeScreen.hidden { display: none; }

  .welcome-card {
    width: 100%;
    max-width: 440px;
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 18px;
    padding: 34px 30px;
    box-shadow: 0 20px 60px rgba(0,0,0,0.45);
    backdrop-filter: blur(6px);
  }

  .welcome-logo {
    width: 56px;
    height: 56px;
    border-radius: 15px;
    background: linear-gradient(135deg, var(--accent-strong), var(--accent));
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 28px;
    margin: 0 auto 16px;
    box-shadow: 0 8px 22px rgba(124, 58, 237, 0.45);
  }

  .welcome-title {
    text-align: center;
    font-size: 26px;
    font-weight: 800;
    letter-spacing: 0.2px;
    margin-bottom: 6px;
  }

  .welcome-tagline {
    text-align: center;
    font-size: 14px;
    color: var(--text-dim);
    margin-bottom: 24px;
    line-height: 1.5;
  }

  .welcome-rules {
    display: flex;
    flex-direction: column;
    gap: 10px;
    margin-bottom: 26px;
  }

  .welcome-rule {
    display: flex;
    align-items: flex-start;
    gap: 10px;
    font-size: 13.5px;
    color: #ded9f0;
    line-height: 1.4;
  }

  .welcome-rule .bullet {
    flex-shrink: 0;
    width: 20px;
    height: 20px;
    border-radius: 6px;
    background: rgba(139,92,246,0.22);
    color: var(--accent-hover);
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 12px;
    margin-top: 1px;
  }

  #enterChatBtn {
    width: 100%;
    padding: 15px;
    border: none;
    border-radius: 10px;
    background: linear-gradient(135deg, var(--accent-strong), var(--accent));
    color: #fff;
    font-size: 15.5px;
    font-weight: 700;
    cursor: pointer;
    box-shadow: 0 8px 22px rgba(124, 58, 237, 0.35);
    transition: filter 0.12s ease, transform 0.12s ease;
  }
  #enterChatBtn:hover { filter: brightness(1.08); transform: translateY(-1px); }
  #enterChatBtn:active { transform: translateY(0); }

  .welcome-remember {
    display: flex;
    align-items: center;
    gap: 8px;
    justify-content: center;
    margin-top: 16px;
    font-size: 12.5px;
    color: var(--text-dim);
  }
  .welcome-remember input { accent-color: var(--accent); width: 15px; height: 15px; }

  @media (max-width: 480px) {
    .welcome-card { padding: 26px 22px; }
    .welcome-title { font-size: 22px; }
  }

  /* =========================================================
     MOBILE (phones — Android & iOS), also covers small tablets
     ========================================================= */
  @media (max-width: 768px) {
    #app {
      position: relative;
      overflow: hidden;
    }

    #menuToggleBtn { display: inline-flex; align-items: center; justify-content: center; }
    #closeSidebarBtn { display: inline-flex; align-items: center; justify-content: center; }

    #sidebar {
      position: fixed;
      top: 0;
      left: 0;
      bottom: 0;
      width: 82vw;
      max-width: 320px;
      min-width: 0;
      z-index: 40;
      transform: translateX(-100%);
      transition: transform 0.25s ease;
      box-shadow: 8px 0 24px rgba(0,0,0,0.4);
      padding-bottom: calc(20px + env(safe-area-inset-bottom));
    }

    #sidebar.open {
      transform: translateX(0);
    }

    #sidebarOverlay {
      display: block;
      position: fixed;
      inset: 0;
      background: rgba(0,0,0,0.5);
      z-index: 30;
      opacity: 0;
      pointer-events: none;
      transition: opacity 0.25s ease;
    }

    #sidebarOverlay.open {
      opacity: 1;
      pointer-events: auto;
    }

    #main {
      width: 100%;
    }

    #chatHeader {
      padding: 0 14px;
      padding-top: env(safe-area-inset-top);
      height: calc(56px + env(safe-area-inset-top));
      gap: 10px;
    }

    #headerTitle {
      font-size: 14px;
      flex-grow: 1;
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    #skipBtn {
      padding: 8px 12px;
      font-size: 12.5px;
      flex-shrink: 0;
    }

    #messages {
      padding: 16px 14px;
    }

    .bubble-wrap {
      max-width: 82%;
    }
    .bubble {
      font-size: 15px;
    }

    #inputBar {
      padding: 12px 14px calc(14px + env(safe-area-inset-bottom));
      padding-left: calc(14px + env(safe-area-inset-left));
      padding-right: calc(14px + env(safe-area-inset-right));
      gap: 8px;
    }

    /* 16px min font-size on inputs prevents iOS Safari auto-zoom on focus */
    #msgInput, #tagInput {
      font-size: 16px;
    }

    #msgInput {
      padding: 12px 14px;
    }

    #sendBtn {
      padding: 0 18px;
      font-size: 14px;
    }

    #findBtn {
      padding: 15px;
      font-size: 15px;
    }

    /* Comfortable 44px+ touch targets */
    #skipBtn, #sendBtn, #findBtn, .tag-chip button {
      min-height: 44px;
    }
    .tag-chip button { min-height: auto; padding: 4px; }

    .empty-state { padding: 0 10px; }
  }

  @media (max-width: 380px) {
    .bubble-wrap { max-width: 88%; }
    .bubble { font-size: 14.5px; }
    .brand-name { font-size: 18px; }
  }
</style>
</head>
<body>

<div id="welcomeScreen">
  <div class="welcome-card">
    <div class="welcome-logo">💬</div>
    <div class="welcome-title">Welcome to Litchat</div>
    <div class="welcome-tagline">Meet random strangers, chat instantly, and stay completely anonymous.</div>

    <div class="welcome-rules">
      <div class="welcome-rule"><span class="bullet">✓</span><span>You must be 18 or older to use Litchat.</span></div>
      <div class="welcome-rule"><span class="bullet">✓</span><span>Be respectful — harassment, hate speech, and spam are not tolerated.</span></div>
      <div class="welcome-rule"><span class="bullet">✓</span><span>Never share personal information with strangers.</span></div>
      <div class="welcome-rule"><span class="bullet">✓</span><span>You can leave a chat and find a new match anytime.</span></div>
    </div>

    <button id="enterChatBtn">Start Chatting</button>

    <label class="welcome-remember">
      <input type="checkbox" id="rememberChoice" />
      Don't show this again on this device
    </label>
  </div>
</div>

<div id="app">

  <!-- SIDEBAR -->
  <div id="sidebar">
    <div class="brand">
      <div class="brand-logo">💬</div>
      <div class="brand-name">Litchat</div>
      <button id="closeSidebarBtn" aria-label="Close menu">✕</button>
    </div>

    <div class="side-block">
      <div class="side-block-label">Total Users Online</div>
      <div id="userCount"><span class="pulse-dot"></span><span id="userCountVal">0</span></div>
    </div>

    <div class="side-block">
      <div class="side-block-label">Status</div>
      <div id="statusLine">Idle</div>
      <div id="statusSub">Hit "Find Stranger" to begin</div>
    </div>

    <div class="side-block tags-wrap">
      <div class="side-block-label">Interest Tags</div>
      <input id="tagInput" type="text" placeholder="e.g. gaming, music, coding" />
      <div id="tagChips"></div>
    </div>

    <div class="sidebar-spacer"></div>

    <button id="findBtn">Find Stranger</button>
  </div>

  <div id="sidebarOverlay"></div>

  <!-- MAIN -->
  <div id="main">
    <div id="chatHeader">
      <button id="menuToggleBtn" aria-label="Open menu">☰</button>
      <div id="headerTitle"><span id="headerStatusDot"></span><span id="headerTitleText">Not connected</span></div>
      <button id="skipBtn" disabled>Next / Skip</button>
    </div>

    <div id="messages">
      <div class="empty-state" id="emptyState">
        <div class="icon">🌌</div>
        <div class="title">Ready when you are</div>
        <div class="sub">Add a few interest tags on the left (optional), then hit "Find Stranger" to get matched instantly.</div>
      </div>
    </div>

    <div id="typingIndicator"></div>

    <div id="inputBar">
      <input id="msgInput" type="text" placeholder="Connect with a stranger to start chatting..." disabled />
      <button id="sendBtn" disabled>Send</button>
    </div>
  </div>

</div>

<script src="/socket.io/socket.io.js"></script>
<script>
  const socket = io();

  // ---- DOM refs ----
  const userCountVal   = document.getElementById('userCountVal');
  const statusLine      = document.getElementById('statusLine');
  const statusSub        = document.getElementById('statusSub');
  const tagInput          = document.getElementById('tagInput');
  const tagChips          = document.getElementById('tagChips');
  const findBtn           = document.getElementById('findBtn');
  const skipBtn            = document.getElementById('skipBtn');
  const sidebar             = document.getElementById('sidebar');
  const sidebarOverlay      = document.getElementById('sidebarOverlay');
  const menuToggleBtn       = document.getElementById('menuToggleBtn');
  const closeSidebarBtn     = document.getElementById('closeSidebarBtn');
  const headerDot          = document.getElementById('headerStatusDot');
  const headerTitleText    = document.getElementById('headerTitleText');
  const messagesEl         = document.getElementById('messages');
  const emptyState         = document.getElementById('emptyState');
  const typingIndicator    = document.getElementById('typingIndicator');
  const msgInput           = document.getElementById('msgInput');
  const sendBtn            = document.getElementById('sendBtn');
  const welcomeScreen      = document.getElementById('welcomeScreen');
  const enterChatBtn       = document.getElementById('enterChatBtn');
  const rememberChoice     = document.getElementById('rememberChoice');

  // ---- State ----
  let tags = [];
  let connected = false;   // paired with a stranger right now
  let searching = false;   // in matchmaking queue
  let typingTimeout = null;
  let partnerTypingTimeout = null;
  let pendingSeenIds = [];  // message ids received while the tab was unfocused

  // ---- Welcome screen ----
  try {
    if (localStorage.getItem('litchat_skip_welcome') === '1') {
      welcomeScreen.classList.add('hidden');
    }
  } catch (e) { /* localStorage unavailable — just show the welcome screen */ }

  enterChatBtn.addEventListener('click', () => {
    if (rememberChoice.checked) {
      try { localStorage.setItem('litchat_skip_welcome', '1'); } catch (e) { /* ignore */ }
    }
    welcomeScreen.classList.add('hidden');
    msgInput && msgInput.blur();
  });

  // ---- Read-receipt tracking (window focus / visibility) ----
  function windowIsActive() {
    return document.hasFocus() && document.visibilityState === 'visible';
  }

  function markSeen(id) {
    if (!id) return;
    if (windowIsActive()) {
      socket.emit('message-seen', { id });
    } else {
      pendingSeenIds.push(id);
    }
  }

  function flushPendingSeen() {
    if (!pendingSeenIds.length) return;
    pendingSeenIds.forEach((id) => socket.emit('message-seen', { id }));
    pendingSeenIds = [];
  }

  window.addEventListener('focus', flushPendingSeen);
  document.addEventListener('visibilitychange', () => {
    if (windowIsActive()) flushPendingSeen();
  });

  function genMessageId() {
    return 'm' + Date.now().toString(36) + Math.random().toString(36).slice(2, 9);
  }

  const SENDING_TICK = '<span class="tick-sending"></span>';
  const SENT_TICK = '<svg viewBox="0 0 16 16" fill="none"><path d="M3 8.5L6.2 11.5L13 4.5" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  const DOUBLE_TICK = '<svg viewBox="0 0 20 16" fill="none"><path d="M1 8.5L4.2 11.5L11 4.5" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/><path d="M6.5 8.5L9.7 11.5L16.5 4.5" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>';

  function setMessageStatus(id, state) {
    const statusEl = messagesEl.querySelector('.msg-status[data-id="' + id + '"]');
    if (!statusEl) return;
    if (state === 'sent') {
      statusEl.innerHTML = SENT_TICK;
      statusEl.classList.remove('seen');
    } else if (state === 'delivered') {
      statusEl.innerHTML = DOUBLE_TICK;
      statusEl.classList.remove('seen');
    } else if (state === 'seen') {
      statusEl.innerHTML = DOUBLE_TICK;
      statusEl.classList.add('seen');
    }
  }

  // ---- Mobile sidebar drawer ----
  function openSidebar() {
    sidebar.classList.add('open');
    sidebarOverlay.classList.add('open');
  }
  function closeSidebar() {
    sidebar.classList.remove('open');
    sidebarOverlay.classList.remove('open');
  }
  menuToggleBtn.addEventListener('click', openSidebar);
  closeSidebarBtn.addEventListener('click', closeSidebar);
  sidebarOverlay.addEventListener('click', closeSidebar);

  // ---- Tag input handling ----
  tagInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' || e.key === ',') {
      e.preventDefault();
      addTagsFromInput();
    }
  });
  tagInput.addEventListener('blur', addTagsFromInput);

  function addTagsFromInput() {
    const raw = tagInput.value.split(',');
    raw.forEach((piece) => {
      const t = piece.trim().toLowerCase();
      if (t && !tags.includes(t) && tags.length < 20) tags.push(t);
    });
    tagInput.value = '';
    renderTagChips();
  }

  function renderTagChips() {
    tagChips.innerHTML = '';
    tags.forEach((t) => {
      const chip = document.createElement('div');
      chip.className = 'tag-chip';
      chip.innerHTML = '<span></span><button type="button" aria-label="remove">✕</button>';
      chip.querySelector('span').textContent = t;
      chip.querySelector('button').addEventListener('click', () => {
        tags = tags.filter((x) => x !== t);
        renderTagChips();
      });
      tagChips.appendChild(chip);
    });
  }

  // ---- Find / Skip button ----
  findBtn.addEventListener('click', () => {
    if (connected || searching) {
      // acts as "stop searching" only if not yet connected
      if (searching && !connected) {
        location.reload();
        return;
      }
    }
    addTagsFromInput();
    startSearch();
  });

  skipBtn.addEventListener('click', () => {
    if (!connected && !searching) return;
    clearMessages();
    addSystemMessage('You skipped the stranger.');
    socket.emit('skip');
    enterSearchingState();
  });

  function startSearch() {
    clearMessages();
    socket.emit('find-stranger', tags);
    enterSearchingState();
    closeSidebar();
  }

  function enterSearchingState() {
    searching = true;
    connected = false;
    findBtn.textContent = 'Searching...';
    findBtn.disabled = true;
    skipBtn.disabled = true;
    headerDot.className = 'searching';
    headerTitleText.textContent = 'Looking for Match...';
    statusLine.textContent = 'Searching';
    statusSub.textContent = tags.length ? ('Matching on: ' + tags.join(', ')) : 'Matching with any stranger';
    msgInput.disabled = true;
    sendBtn.disabled = true;
    msgInput.placeholder = 'Waiting for a stranger to connect...';
    typingIndicator.textContent = '';
  }

  // ---- Socket events ----
  socket.on('user-count', (count) => {
    userCountVal.textContent = count;
  });

  socket.on('searching', () => {
    enterSearchingState();
  });

  socket.on('matched', () => {
    searching = false;
    connected = true;
    findBtn.textContent = 'New Stranger';
    findBtn.disabled = false;
    skipBtn.disabled = false;
    headerDot.className = 'connected';
    headerTitleText.textContent = 'Chatting with Stranger';
    statusLine.textContent = 'Connected';
    statusSub.textContent = 'You are now chatting with a stranger';
    msgInput.disabled = false;
    sendBtn.disabled = false;
    msgInput.placeholder = 'Type a message...';
    msgInput.focus();
    clearMessages();
    addSystemMessage('You are now chatting with a random stranger. Say hi!');
    pendingSeenIds = [];
  });

  socket.on('partner-left', () => {
    connected = false;
    pendingSeenIds = [];
    addSystemMessage('Stranger has disconnected.');
    headerDot.className = '';
    headerTitleText.textContent = 'Not connected';
    statusLine.textContent = 'Idle';
    statusSub.textContent = 'Hit "Find Stranger" to begin';
    msgInput.disabled = true;
    sendBtn.disabled = true;
    msgInput.placeholder = 'Connect with a stranger to start chatting...';
    findBtn.textContent = 'Find Stranger';
    findBtn.disabled = false;
    skipBtn.disabled = true;
    typingIndicator.textContent = '';
  });

  socket.on('chat-message', (payload) => {
    const text = payload && typeof payload === 'object' ? payload.text : payload;
    const id = payload && typeof payload === 'object' ? payload.id : null;
    addMessage(text, 'stranger', id);
    typingIndicator.textContent = '';
    markSeen(id);
  });

  socket.on('message-delivered', ({ id }) => {
    setMessageStatus(id, 'delivered');
  });

  socket.on('message-seen', ({ id }) => {
    setMessageStatus(id, 'seen');
  });

  socket.on('typing', () => {
    typingIndicator.textContent = 'Stranger is typing...';
    clearTimeout(partnerTypingTimeout);
    partnerTypingTimeout = setTimeout(() => {
      typingIndicator.textContent = '';
    }, 3000);
  });

  socket.on('stop-typing', () => {
    typingIndicator.textContent = '';
    clearTimeout(partnerTypingTimeout);
  });

  // ---- Sending messages ----
  function sendMessage() {
    const text = msgInput.value.trim();
    if (!text || !connected) return;
    const id = genMessageId();
    socket.emit('chat-message', { id, text });
    addMessage(text, 'you', id);
    msgInput.value = '';
    socket.emit('stop-typing');
    clearTimeout(typingTimeout);
  }

  sendBtn.addEventListener('click', sendMessage);
  msgInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      sendMessage();
    }
  });

  msgInput.addEventListener('input', () => {
    if (!connected) return;
    if (msgInput.value.length === 0) {
      socket.emit('stop-typing');
      clearTimeout(typingTimeout);
      return;
    }
    socket.emit('typing');
    clearTimeout(typingTimeout);
    typingTimeout = setTimeout(() => {
      socket.emit('stop-typing');
    }, 1500);
  });

  // ---- Message rendering ----
  function clearMessages() {
    messagesEl.innerHTML = '';
  }

  function addMessage(text, from, id) {
    if (emptyState.parentNode) emptyState.remove();
    const row = document.createElement('div');
    row.className = 'msg-row ' + from;

    const wrap = document.createElement('div');
    wrap.className = 'bubble-wrap';

    const bubble = document.createElement('div');
    bubble.className = 'bubble';
    bubble.textContent = text;
    wrap.appendChild(bubble);

    if (from === 'you' && id) {
      const status = document.createElement('div');
      status.className = 'msg-status';
      status.setAttribute('data-id', id);
      status.innerHTML = SENDING_TICK;
      wrap.appendChild(status);
      // Mark as "sent" the instant it's queued on the wire.
      requestAnimationFrame(() => setMessageStatus(id, 'sent'));
    }

    row.appendChild(wrap);
    messagesEl.appendChild(row);
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }

  function addSystemMessage(text) {
    if (emptyState.parentNode) emptyState.remove();
    const div = document.createElement('div');
    div.className = 'msg-system';
    div.textContent = text;
    messagesEl.appendChild(div);
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }
</script>
</body>
</html>`;
}

// ---------------------------------------------------------------------------

server.listen(PORT, () => {
  console.log(`Litchat server running on port ${PORT}`);
});
