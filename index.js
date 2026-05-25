import 'dotenv/config';
import http from 'http';
import { Server } from 'socket.io';
import cron  from 'node-cron';
import cors  from 'cors';
import mongoose from 'mongoose';
import Redis from 'ioredis';     
import Room from './models/msgs.jd';
import express  from 'express';

const app = express();
const server = http.createServer(app);

app.use(cors());
app.use(express.json());

// socket.io
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'], credentials: true }
});

// ── Redis client (ioredis)
// ioredis auto-reconnects; no need to manually handle connection events
const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

redis.on('connect', () => console.log('✅ Redis connected'));
redis.on('error',   (err) => console.error('❌ Redis error:', err.message));

mongoose
  .connect(process.env.MONGO_URI)
  .then(() => console.log('DB connected'))
  .catch((err) => console.error('MongoDB error:', err.message));

// ── Health check route 
app.get('/', (_req, res) => {
  res.send('<h1>Relay Chat Server — Running ✅</h1>');
});

// ── Socket.io events 
io.on('connection', (socket) => {
  console.log(`⚡ User connected: ${socket.id}`);

  // ── JOIN: client joins a named room ──────────────────────────
  socket.on('join', async (roomId) => {
    socket.join(roomId);
    console.log(`  → ${socket.id} joined room "${roomId}"`);

    try {
      // 1. Check Redis cache first (fast path)
      //    Messages are stored as JSON strings in a Redis list
      const cached = await redis.lrange(`room:${roomId}`, 0, -1);

      if (cached.length > 0) {
        // Redis list is ordered newest-first (LPUSH), so reverse for chronological
        const history = cached.map(s => JSON.parse(s)).reverse();
        socket.emit('history', history);           // send only to the joining socket
        return;
      }

      // 2. Fallback: fetch from MongoDB
      const roomDoc = await Room.findOne({ roomId });
      if (roomDoc?.messages?.length) {
        // Re-populate Redis for next join (optional but speeds up future joins)
        const pipeline = redis.pipeline();
        for (const msg of roomDoc.messages) {
          pipeline.lpush(`room:${roomId}`, JSON.stringify({
            username: msg.senderName,
            text:     msg.message,
            room:     roomId,
            timestamp: msg.timestamp,
          }));
        }
        pipeline.expire(`room:${roomId}`, 5400); // 90 min TTL
        await pipeline.exec();

        socket.emit('history', roomDoc.messages.map(m => ({
          username:  m.senderName,
          text:      m.message,
          room:      roomId,
          timestamp: m.timestamp,
        })));
      }
    } catch (err) {
      console.error('Error loading history:', err.message);
    }
  });

  socket.on('leave', (roomId) => {
    socket.leave(roomId);
    console.log(`  ← ${socket.id} left room "${roomId}"`);
  });

  // broadcast and cache message 
  socket.on('send', async (msg) => {
    const { text, room, username } = msg;

    if (!text?.trim() || !room || !username) return;

    const payload = {
      text:      text.trim(),
      room,
      username,
      timestamp: new Date().toISOString(),
    };

    // Broadcast to everyone else in the room (not sender — sender adds optimistically)
    socket.to(room).emit('message', payload);

    // Push to Redis list — LPUSH stores newest at index 0
    try {
      await redis.lpush(`room:${room}`, JSON.stringify(payload));
      await redis.expire(`room:${room}`, 5400); // reset 90-min TTL on activity
    } catch (err) {
      console.error('Redis LPUSH error:', err.message);
    }
  });

  // msg shape: { room, msgId, newText, username }
  socket.on('edit', async ({ room, msgId, newText, username }) => {
    try {
      // Validate and cast the MongoDB ObjectId before updating
      const objectId = new mongoose.Types.ObjectId(msgId); // throws if invalid
      await Room.updateOne(
        { roomId: room, 'messages._id': objectId, 'messages.senderName': username },
        { $set: { 'messages.$.message': newText.trim() } }
      );
      io.to(room).emit('message:edited', { msgId, newText: newText.trim() });
    } catch (err) {
      console.error('Edit error:', err.message);
      socket.emit('error', { event: 'edit', message: 'Could not edit message.' });
    }
  });

  // msg shape: { room, msgId, username }
  socket.on('delete', async ({ room, msgId, username }) => {
    try {
      const objectId = new mongoose.Types.ObjectId(msgId);
      await Room.updateOne(
        { roomId: room, 'messages.senderName': username },
        { $pull: { messages: { _id: objectId } } }
      );
      io.to(room).emit('message:deleted', { msgId });
    } catch (err) {
      console.error('Delete error:', err.message);
      socket.emit('error', { event: 'delete', message: 'Could not delete message.' });
    }
  });

  socket.on('disconnect', () => {
    console.log(`User disconnected: ${socket.id}`);
  });
});

cron.schedule('0 */2 * * *', async () => {
  console.log('Cron: flushing Redis cache → MongoDB...');

  try {
    // Find all Redis keys that match our pattern
    // KEYS is fine for low-traffic; use SCAN for production at scale
    const keys = await redis.keys('room:*');

    for (const key of keys) {
      const roomId   = key.replace('room:', '');
      const rawMsgs  = await redis.lrange(key, 0, -1); // fetch entire list
      if (!rawMsgs.length) continue;

      const messages = rawMsgs.map(s => {
        const p = JSON.parse(s);
        return {
          senderName: p.username,
          message:    p.text,
          timestamp:  p.timestamp ? new Date(p.timestamp) : new Date(),
        };
      }).reverse(); // oldest first

      // Upsert: create the room doc if it doesn't exist, then push all messages
      await Room.findOneAndUpdate(
        { roomId },
        { $push: { messages: { $each: messages } } },
        { upsert: true, new: true }
      );

      // Clear Redis list after successful write
      await redis.del(key);
      console.log(` Flushed "${roomId}" — ${messages.length} messages`);
    }
  } catch (err) {
    console.error('Cron flush error:', err.message);
  }
});

const PORT = process.env.PORT || 5050;
server.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
