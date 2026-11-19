import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { distribution } from "../../lib/runtime-load-scheduler.mjs";

export const wall = () => performance.timeOrigin + performance.now();

// Merge the captured lanes by original creation time without moving a record
// ahead of an already delivered offset on its own partition.
export function orderFrames(frames) {
  const last = new Map();
  for (const frame of frames) {
    const prior = last.get(frame.partition);
    assert(!prior || prior.offset < frame.offset);
    frame.orderTime = Math.max(frame.created, prior?.orderTime ?? 0);
    last.set(frame.partition, frame);
  }
  return frames.sort(
    (a, b) => a.orderTime - b.orderTime || a.partition - b.partition || a.offset - b.offset,
  );
}

// Fixed open-loop arrivals. Backpressure is observable as sender lateness.
// Every scheduled frame is delivered, including those that finish after the offer.
export async function pace(frames, rate, start, offer) {
  const late = [];
  for (let i = 0; i < frames.length;) {
    const now = wall();
    const due = start + (i * 1000) / rate;
    if (due > now) {
      await delay(Math.min(25, Math.max(1, due - now)));
      continue;
    }
    const until = Math.min(frames.length, i + 128, Math.floor(((now - start) * rate) / 1000) + 1);
    for (; i < until; i++) {
      late.push(Math.max(0, wall() - start - (i * 1000) / rate));
      const wait = offer(frames[i], i);
      if (wait) await wait;
    }
  }
  return { frames: late.length, lateMs: distribution(late) };
}

export async function emitFrames(frames, producer, rate, start) {
  const pending = new Set();
  let error;
  const result = await pace(frames, rate, start, (f) => {
    if (error) throw error;
    const delivery = producer
      .append(f.partition, { factId: f.id, sessionId: f.session, value: f.value })
      .catch((e) => {
        error ??= e;
      })
      .finally(() => pending.delete(delivery));
    pending.add(delivery);
    if (pending.size >= 512) return Promise.race(pending);
  });
  await Promise.all(pending);
  if (error) throw error;
  return result;
}
