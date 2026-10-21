const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const path = require("path");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
});

const PORT = process.env.PORT || 3000;

// Serve public static frontend files safely
app.use(express.static(__dirname));

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

// A pool of cool temporary identities for professional users
const animals = [
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

function generateIdentity() {
  const adj = adjectives[Math.floor(Math.random() * adjectives.length)];
  const animal = animals[Math.floor(Math.random() * animals.length)];
  const hash = Math.floor(1000 + Math.random() * 9000);
  return { username: `${adj} ${animal}`, tag: `#${hash}` };
}

// Global state holding the single waiting socket
let waitingSocket = null;

io.on("connection", (socket) => {
  // Generate private tracking state for this specific client session
  socket.identity = generateIdentity();

  // Transmit unique generated credentials back down to user interface dashboard
  socket.emit("session-created", socket.identity);

  // Event listener: Matchmaking algorithm sequence
  socket.on("find-match", () => {
    // Guard clause: Prevent a user from matching with themselves if clicking multiple times
    if (waitingSocket && waitingSocket.id === socket.id) {
      return;
    }

    // Check if there is an active user waiting inside the pool queue
    if (waitingSocket && io.sockets.sockets.has(waitingSocket.id)) {
      const roomName = `room_${waitingSocket.id}_${socket.id}`;

      // Link both independent channels into an isolated chat room partition
      socket.join(roomName);
      waitingSocket.join(roomName);

      // Cross-reference current operational rooms onto socket schemas
      socket.activeRoom = roomName;
      waitingSocket.activeRoom = roomName;

      // Link them as peers so they can reference each other directly
      socket.peerSocket = waitingSocket;
      waitingSocket.peerSocket = socket;

      // Notify client layers to transition UI frameworks into active chat dashboards
      socket.emit("matched", { peer: waitingSocket.identity });
      waitingSocket.emit("matched", { peer: socket.identity });

      // Empty global container reference so new incoming strangers can pair up
      waitingSocket = null;
    } else {
      // Pool empty: Append current unique session reference into waiting queue block
      waitingSocket = socket;
      socket.emit("waiting");
    }
  });

  // Event listener: Secure end-to-end messaging transit pipes
  socket.on("send-chat-message", (payload) => {
    if (socket.activeRoom && payload && payload.trim() !== "") {
      // Emits raw sanitized message string to the specific target room
      socket.to(socket.activeRoom).emit("chat-message", {
        text: payload.trim(),
      });
    }
  });

  // Event listener: Clean session closing handlers
  socket.on("leave-chat", () => {
    handleDisconnections(socket);
  });

  socket.on("disconnect", () => {
    handleDisconnections(socket);
  });
});

function handleDisconnections(socket) {
  // If this specific user was standing inside the pool line, empty the queue container
  if (waitingSocket && waitingSocket.id === socket.id) {
    waitingSocket = null;
  }

  // Clean up active peer state and safely clean isolated variables
  if (socket.activeRoom) {
    io.to(socket.activeRoom).emit("peer-disconnected");

    if (socket.peerSocket) {
      socket.peerSocket.leave(socket.activeRoom);
      socket.peerSocket.activeRoom = null;
      socket.peerSocket.peerSocket = null;
    }

    socket.leave(socket.activeRoom);
    socket.activeRoom = null;
    socket.peerSocket = null;
  }
}

server.listen(PORT, () => {
  console.log(`\n🚀 =================================================== 🚀`);
  console.log(`   Stratosphere Core Server Engine Online Engine Running`);
  console.log(`   Direct Link Access Point -> http://localhost:${PORT}`);
  console.log(`🚀 =================================================== 🚀\n`);
});
