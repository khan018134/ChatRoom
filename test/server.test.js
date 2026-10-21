const assert = require("node:assert/strict");
const { after, afterEach, before, test } = require("node:test");
const fs = require("node:fs");
const path = require("node:path");
const { io: createClient } = require("socket.io-client");
const { createChatServer } = require("../server");

const clients = new Set();
let context;
let baseUrl;

function waitForEvent(socket, eventName, timeoutMs = 2000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`Timed out waiting for ${eventName}`));
    }, timeoutMs);

    const onEvent = (...args) => {
      cleanup();
      resolve(args);
    };
    const onDisconnect = (reason) => {
      cleanup();
      reject(new Error(`Disconnected before ${eventName}: ${reason}`));
    };
    const cleanup = () => {
      clearTimeout(timer);
      socket.off(eventName, onEvent);
      socket.off("disconnect", onDisconnect);
    };

    socket.once(eventName, onEvent);
    socket.once("disconnect", onDisconnect);
  });
}

async function connectClient(url = baseUrl, options = {}) {
  const client = createClient(url, {
    autoConnect: false,
    reconnection: false,
    transports: ["websocket"],
    ...options,
  });
  clients.add(client);
  const identity = waitForEvent(client, "session-created");
  client.connect();
  const [data] = await identity;
  return { client, identity: data };
}

function emitWithAck(socket, eventName, ...args) {
  return new Promise((resolve) => {
    socket.emit(eventName, ...args, resolve);
  });
}

function waitForEventValue(socket, eventName, expectedValue, timeoutMs = 2000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.off(eventName, onEvent);
      reject(new Error(`Timed out waiting for ${eventName}=${expectedValue}`));
    }, timeoutMs);
    const onEvent = (value) => {
      if (value !== expectedValue) return;
      clearTimeout(timer);
      socket.off(eventName, onEvent);
      resolve(value);
    };
    socket.on(eventName, onEvent);
  });
}

async function waitForNoServerSockets() {
  const deadline = Date.now() + 1000;
  while (context.io.sockets.sockets.size > 0 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.equal(context.io.sockets.sockets.size, 0, "test clients should disconnect");
}

before(async () => {
  context = createChatServer();
  await new Promise((resolve, reject) => {
    context.server.once("error", reject);
    context.server.listen(0, "127.0.0.1", resolve);
  });
  baseUrl = `http://127.0.0.1:${context.server.address().port}`;
});

afterEach(async () => {
  for (const client of clients) client.disconnect();
  await waitForNoServerSockets();
  clients.clear();
});

after(async () => {
  await new Promise((resolve) => context.io.close(resolve));
});

test("frontend document is well formed and its inline script is safe to parse", () => {
  const html = fs.readFileSync(path.join(__dirname, "..", "index.html"), "utf8");
  const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)];
  assert.match(html, /<\/head>\s*<body>/);
  assert.match(html, /<\/body>\s*<\/html>\s*$/);
  assert.equal(scripts.length, 1);
  assert.doesNotThrow(() => new Function(scripts[0][1]));
  assert.doesNotMatch(scripts[0][1], /\.innerHTML\s*=/);
});

test("serves only the chat page and sets browser security headers", async () => {
  const response = await fetch(baseUrl);
  assert.equal(response.status, 200);
  assert.match(await response.text(), /<html lang="en">/);
  assert.equal(response.headers.get("x-content-type-options"), "nosniff");
  assert.equal(response.headers.get("x-frame-options"), "DENY");

  for (const file of [
    "/server.js",
    "/package.json",
    "/package-lock.json",
    "/tailwind.js",
    "/tailwind.html",
  ]) {
    const hiddenFile = await fetch(`${baseUrl}${file}`);
    assert.equal(hiddenFile.status, 404, `${file} should not be served`);
  }
});

test("matches independent peers, isolates rooms, and validates messages", async () => {
  const [alice, bob, charlie] = await Promise.all([
    connectClient(),
    connectClient(),
    connectClient(),
  ]);
  assert.notEqual(alice.identity.tag, bob.identity.tag);
  assert.notEqual(alice.identity.tag, charlie.identity.tag);

  const aliceWaiting = waitForEvent(alice.client, "waiting");
  alice.client.emit("find-match");
  await aliceWaiting;

  for (let attempt = 0; attempt < 5; attempt += 1) {
    alice.client.emit("find-match");
  }
  const aliceMatched = waitForEvent(alice.client, "matched");
  const bobMatched = waitForEvent(bob.client, "matched");
  bob.client.emit("find-match");
  const [[aliceData], [bobData]] = await Promise.all([aliceMatched, bobMatched]);
  assert.equal(aliceData.peer.username, bob.identity.username);
  assert.equal(bobData.peer.username, alice.identity.username);

  const chatMessage = waitForEvent(bob.client, "chat-message");
  const validMessage = await emitWithAck(
    alice.client,
    "send-chat-message",
    "<script>alert(1)</script>",
  );
  assert.deepEqual(validMessage, { ok: true });
  assert.deepEqual((await chatMessage)[0], { text: "<script>alert(1)</script>" });

  for (const payload of ["   ", { text: "spoofed" }, "x".repeat(2001)]) {
    const result = await emitWithAck(alice.client, "send-chat-message", payload);
    assert.equal(result.ok, false);
  }

  const thirdWaiting = waitForEvent(charlie.client, "waiting");
  charlie.client.emit("find-match");
  await thirdWaiting;

  const noLeak = waitForEvent(charlie.client, "chat-message", 150);
  alice.client.emit("send-chat-message", "private to Bob", () => {});
  await assert.rejects(noLeak, /Timed out waiting for chat-message/);

  const aliceDisconnected = waitForEvent(alice.client, "peer-disconnected");
  const charlieMatched = waitForEvent(charlie.client, "matched");
  const aliceReMatched = waitForEvent(alice.client, "matched");
  bob.client.disconnect();
  await aliceDisconnected;
  alice.client.emit("find-match");
  const [[charlieData], [aliceDataAgain]] = await Promise.all([
    charlieMatched,
    aliceReMatched,
  ]);
  assert.equal(charlieData.peer.username, alice.identity.username);
  assert.equal(aliceDataAgain.peer.username, charlie.identity.username);

  const charlieDisconnected = waitForEvent(charlie.client, "peer-disconnected");
  alice.client.emit("leave-chat");
  await charlieDisconnected;
  alice.client.emit("leave-chat");
});

test("cleans up disconnected queue entries and rejects foreign origins", async () => {
  const queued = await connectClient();
  const queuedEvent = waitForEvent(queued.client, "waiting");
  queued.client.emit("find-match");
  await queuedEvent;
  queued.client.disconnect();

  const next = await connectClient();
  const nextWaiting = waitForEvent(next.client, "waiting");
  next.client.emit("find-match");
  await nextWaiting;

  const rejected = createClient(baseUrl, {
    autoConnect: false,
    reconnection: false,
    transports: ["websocket"],
    extraHeaders: { Origin: "https://untrusted.example" },
  });
  clients.add(rejected);
  const connectionError = waitForEvent(rejected, "connect_error");
  rejected.connect();
  await connectionError;
  rejected.disconnect();
});

test("reconnects with a fresh identity and no stale queue entry", async () => {
  const original = await connectClient();
  const firstWaiting = waitForEvent(original.client, "waiting");
  original.client.emit("find-match");
  await firstWaiting;

  const disconnected = waitForEvent(original.client, "disconnect");
  original.client.disconnect();
  await disconnected;
  await waitForNoServerSockets();

  const peer = await connectClient();
  const peerWaiting = waitForEvent(peer.client, "waiting");
  peer.client.emit("find-match");
  await peerWaiting;

  const reconnectedIdentity = waitForEvent(original.client, "session-created");
  const reconnected = waitForEvent(original.client, "connect");
  original.client.connect();
  const [[identity]] = await Promise.all([reconnectedIdentity, reconnected]);
  assert.match(identity.tag, /^#[A-F0-9]{6}$/);

  const peerMatched = waitForEvent(peer.client, "matched");
  const originalMatched = waitForEvent(original.client, "matched");
  original.client.emit("find-match");
  const [[peerData], [originalData]] = await Promise.all([
    peerMatched,
    originalMatched,
  ]);
  assert.equal(peerData.peer.username, identity.username);
  assert.equal(originalData.peer.username, peer.identity.username);
});

test("limits rapid messages and rejects invalid typing payloads", async () => {
  const [sender, receiver] = await Promise.all([connectClient(), connectClient()]);
  const senderMatched = waitForEvent(sender.client, "matched");
  const receiverMatched = waitForEvent(receiver.client, "matched");
  const senderWaiting = waitForEvent(sender.client, "waiting");
  sender.client.emit("find-match");
  await senderWaiting;
  receiver.client.emit("find-match");
  await Promise.all([senderMatched, receiverMatched]);

  const results = [];
  for (let index = 0; index < 11; index += 1) {
    results.push(
      await emitWithAck(sender.client, "send-chat-message", `message ${index}`),
    );
  }
  assert.equal(results.filter((result) => result.ok).length, 10);
  assert.equal(results.at(-1).ok, false);

  const invalidTyping = waitForEvent(sender.client, "action-error");
  sender.client.emit("typing-signal", "true");
  assert.match((await invalidTyping)[0].message, /Invalid typing signal/);

  for (let index = 0; index < 29; index += 1) {
    sender.client.emit("typing-signal", true);
    sender.client.emit("typing-signal", false);
  }
  await new Promise((resolve) => setTimeout(resolve, 50));
  const typingStopped = waitForEventValue(receiver.client, "peer-typing", false);
  sender.client.emit("typing-signal", true);
  sender.client.emit("typing-signal", false);
  assert.equal(await typingStopped, false);

  const typingRateLimit = waitForEvent(sender.client, "action-error");
  sender.client.emit("typing-signal", true);
  assert.match((await typingRateLimit)[0].message, /Too many requests/);
});
