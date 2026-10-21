const crypto = require("crypto");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const path = require("path");

const MAX_MESSAGE_LENGTH = 2000;
const RATE_LIMITS = {
  matchmaking: { max: 5, windowMs: 10_000 },
  messages: { max: 10, windowMs: 5_000 },
  typing: { max: 30, windowMs: 10_000 },
};

const roles = [
  "Architect",
  "Engineer",
  "Designer",
  "Strategist",
  "Developer",
  "Analyst",
  "Founder",
  "Innovator",
];
const adjectives = [
  "Anonymous",
  "Creative",
  "Technical",
  "Strategic",
  "Digital",
  "Global",
  "Agile",
  "Visionary",
];

function parseAllowedOrigins(value = process.env.ALLOWED_ORIGINS || "") {
  const origins = new Set();

  for (const entry of value.split(",")) {
    const candidate = entry.trim();
    if (!candidate) continue;

    let origin;
    try {
      origin = new URL(candidate);
    } catch {
      throw new Error(`Invalid origin in ALLOWED_ORIGINS: ${candidate}`);
    }

    if (
      !["http:", "https:"].includes(origin.protocol) ||
      origin.origin !== candidate
    ) {
      throw new Error(`Invalid origin in ALLOWED_ORIGINS: ${candidate}`);
    }
    origins.add(origin.origin);
  }

  return origins;
}

function isAllowedOrigin(origin, host, allowedOrigins) {
  if (!origin) return true;
  if (allowedOrigins.has(origin)) return true;

  try {
    const parsedOrigin = new URL(origin);
    return (
      parsedOrigin.origin === origin &&
      Boolean(host) &&
      parsedOrigin.host.toLowerCase() === host.toLowerCase()
    );
  } catch {
    return false;
  }
}

function generateIdentity(usedTags) {
  let tag;
  do {
    tag = `#${crypto.randomBytes(3).toString("hex").toUpperCase()}`;
  } while (usedTags.has(tag));
  usedTags.add(tag);

  const adjective = adjectives[crypto.randomInt(adjectives.length)];
  const role = roles[crypto.randomInt(roles.length)];
  return { username: `${adjective} ${role}`, tag };
}

function createChatServer({ allowedOrigins = parseAllowedOrigins() } = {}) {
  const app = express();
  const server = http.createServer(app);
  const socketOptions = {
    allowRequest: (request, callback) => {
      callback(
        null,
        isAllowedOrigin(
          request.headers.origin,
          request.headers.host,
          allowedOrigins,
        ),
      );
    },
  };

  if (allowedOrigins.size > 0) {
    socketOptions.cors = { origin: [...allowedOrigins] };
  }

  const io = new Server(server, socketOptions);
  const usedTags = new Set();
  let waitingSocket = null;

  app.disable("x-powered-by");
  app.use((request, response, next) => {
    response.set({
      "Content-Security-Policy":
        "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self' ws: wss:; img-src 'self' data:; object-src 'none'; base-uri 'self'; frame-ancestors 'none'",
      "Permissions-Policy": "camera=(), geolocation=(), microphone=()",
      "Referrer-Policy": "no-referrer",
      "X-Content-Type-Options": "nosniff",
      "X-Frame-Options": "DENY",
    });
    next();
  });

  app.get(["/", "/index.html"], (request, response) => {
    response.sendFile(path.join(__dirname, "index.html"));
  });

  app.use((request, response) => {
    response.status(404).type("text/plain").send("Not found");
  });

  app.use((error, request, response, next) => {
    if (response.headersSent) return next(error);
    console.error("HTTP request failed:", error.message);
    response.status(500).type("text/plain").send("Internal server error");
  });

  function notifyActionError(socket, message) {
    socket.emit("action-error", { message });
  }

  function allowEvent(socket, eventName, limit, shouldNotify = true) {
    const now = Date.now();
    const rateLimits = socket.data.rateLimits;
    const recentEvents = (rateLimits.get(eventName) || []).filter(
      (timestamp) => now - timestamp < limit.windowMs,
    );

    if (recentEvents.length >= limit.max) {
      rateLimits.set(eventName, recentEvents);
      if (shouldNotify) {
        notifyActionError(socket, "Too many requests. Please wait and try again.");
      }
      return false;
    }

    recentEvents.push(now);
    rateLimits.set(eventName, recentEvents);
    return true;
  }

  function endSession(socket) {
    if (waitingSocket === socket) {
      waitingSocket = null;
    }

    const room = socket.data.activeRoom;
    if (!room) return;

    const peer = io.sockets.sockets.get(socket.data.peerId);
    socket.to(room).emit("peer-disconnected");
    socket.data.isTyping = false;

    if (peer) {
      peer.leave(room);
      delete peer.data.activeRoom;
      delete peer.data.peerId;
      peer.data.isTyping = false;
    }

    socket.leave(room);
    delete socket.data.activeRoom;
    delete socket.data.peerId;
  }

  io.on("connection", (socket) => {
    const identity = generateIdentity(usedTags);
    socket.data.identity = identity;
    socket.data.rateLimits = new Map();
    socket.emit("session-created", identity);

    socket.on("find-match", () => {
      if (socket.data.activeRoom || waitingSocket === socket) return;
      if (!allowEvent(socket, "matchmaking", RATE_LIMITS.matchmaking)) return;

      if (
        waitingSocket &&
        (!io.sockets.sockets.has(waitingSocket.id) ||
          waitingSocket.data.activeRoom)
      ) {
        waitingSocket = null;
      }

      if (!waitingSocket) {
        waitingSocket = socket;
        socket.emit("waiting");
        return;
      }

      const peer = waitingSocket;
      waitingSocket = null;
      const room = `chat:${crypto.randomUUID()}`;
      socket.join(room);
      peer.join(room);
      socket.data.activeRoom = room;
      socket.data.peerId = peer.id;
      socket.data.isTyping = false;
      peer.data.activeRoom = room;
      peer.data.peerId = socket.id;
      peer.data.isTyping = false;

      socket.emit("matched", { peer: peer.data.identity });
      peer.emit("matched", { peer: identity });
    });

    socket.on("send-chat-message", (payload, acknowledge) => {
      const rejectMessage = (message) => {
        if (typeof acknowledge === "function") {
          acknowledge({ ok: false, error: message });
        }
        socket.emit("chat-error", { message });
      };

      if (typeof payload !== "string") {
        rejectMessage("Messages must be plain text.");
        return;
      }

      const text = payload.trim();
      if (!text) {
        rejectMessage("Enter a message before sending.");
        return;
      }
      if (text.length > MAX_MESSAGE_LENGTH) {
        rejectMessage(`Messages must be ${MAX_MESSAGE_LENGTH} characters or fewer.`);
        return;
      }

      const room = socket.data.activeRoom;
      if (!room || !socket.data.peerId) {
        rejectMessage("Connect to a peer before sending a message.");
        return;
      }
      if (!allowEvent(socket, "messages", RATE_LIMITS.messages, false)) {
        if (typeof acknowledge === "function") {
          acknowledge({
            ok: false,
            error: "Too many messages. Please wait and try again.",
          });
        } else {
          socket.emit("chat-error", {
            message: "Too many messages. Please wait and try again.",
          });
        }
        return;
      }

      socket.to(room).emit("chat-message", { text });
      if (typeof acknowledge === "function") acknowledge({ ok: true });
    });

    socket.on("typing-signal", (isTyping) => {
      if (typeof isTyping !== "boolean") {
        notifyActionError(socket, "Invalid typing signal.");
        return;
      }

      const room = socket.data.activeRoom;
      if (!room || !socket.data.peerId) return;
      if (socket.data.isTyping === isTyping) return;
      if (isTyping && !allowEvent(socket, "typing", RATE_LIMITS.typing)) return;
      socket.data.isTyping = isTyping;
      socket.to(room).emit("peer-typing", isTyping);
    });

    socket.on("leave-chat", () => {
      endSession(socket);
    });

    socket.on("disconnect", () => {
      endSession(socket);
      usedTags.delete(identity.tag);
    });
  });

  return { app, server, io };
}

function startServer() {
  const { server, io } = createChatServer();
  const port = Number(process.env.PORT || 3000);
  const host = process.env.HOST || "127.0.0.1";

  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("PORT must be an integer between 1 and 65535.");
  }

  server.on("error", (error) => {
    console.error("Server failed to start:", error.message);
    process.exitCode = 1;
  });
  server.listen(port, host, () => {
    console.log(`Local access: http://127.0.0.1:${port}`);
    console.log(`Network binding: ${host}:${port}`);
  });

  let shuttingDown = false;
  const shutdown = () => {
    if (shuttingDown) return;
    shuttingDown = true;
    io.close(() => {
      console.log("ChatRoom server stopped.");
    });
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  return { server, io };
}

if (require.main === module) {
  startServer();
}

module.exports = {
  MAX_MESSAGE_LENGTH,
  RATE_LIMITS,
  createChatServer,
  parseAllowedOrigins,
  startServer,
};
