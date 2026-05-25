// Schema design:
//   One document per room, messages embedded as a sub-array.
//   This is efficient for reads (single query per room) but
//   MongoDB has a 16 MB document limit — for very high-traffic rooms consider switching to a separate Messages collection.

import mongoose from 'mongoose';

const msgSchema = new mongoose.Schema(
  {
    senderName: {
      type:     String,
      required: true,
      trim:     true,
    },
    message: {
      type:     String,
      required: true,
      trim:     true,
    },
    timestamp: {
      type:    Date,
      default: Date.now,
    },
  },
  {
    // Mongoose automatically adds _id to sub-docs, We need this for edit/delete by ObjectId — keep it (default  true)
    _id: true,
  }
);

// Main schema: one document per room 
const roomSchema = new mongoose.Schema(
  {
    roomId: {
      type:     String,
      required: true,
      unique:   true,
      trim:     true,
      lowercase: true, 
    },
    messages: {
      type:    [msgSchema],
      default: [],
    },
  },
  {
    timestamps: true,
  }
);

export default mongoose.model('Room', roomSchema);