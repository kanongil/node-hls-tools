import { Transform } from 'stream';

import { BufferList } from 'bl';

import { TimedChunk } from './collect.mjs';
import { TimeStamp } from './utils.mjs';
import { assert, wait } from 'hls-playlist-reader/helpers';


const internals = {
  timeOffset: Date.now() - performance.now()
};


internals.limitedWait = function(waitMs) {

  assert(waitMs < 10_000, `Unexpectedly high wait time: ${waitMs}ms`)

  if (waitMs > 0)
    return wait(Math.round(waitMs));
};


/**
 * The returned time is somewhat accurate. More importantly, the deltas are quite accurate.
 * 
 * Monotonic, should account for drift.
 */
internals.accurateNow = function () {

  return internals.timeOffset + performance.now();
};


export class Smoother extends Transform {

  /** Maximum size of output packets in bytes. @type {number} */
  packetSize;

  limits = {
    late: 40,
    future: 10_000
  };

  lateLimit = 40;

  dropped = {
    late: 0,
    future: 0
  };

  //bitrate = 10E06;

  /** @type {{ ts: TimeStamp; time: number } | undefined} */
  #ref;

  /** Whether time is based on stream PCR */
  #streamSync = true;

  constructor(options) {

    options = options || {};

    const packetSize = options.packetSize || 7 * 188;

    super({
      writableObjectMode: true,
      writableHighWaterMark: 1,
      readableHighWaterMark: packetSize
    });

    this.packetSize = options.packetSize || 7 * 188; // size of output packets
  }

  /**
   * @param {TimeStamp} timestamp
   * @param {number} date
   * @return {number | undefined} Projected output time
   */
  #outputTime(timestamp, date) {

    // when this is called normally, now ~= this.#ref.time

    if (timestamp === undefined) return undefined;

    if (!this.#ref ||
        (date && date !== this.#ref.time)) {

      this.#ref = {
        ts: timestamp,
        time: date ? date : internals.accurateNow()
      };
      this.#streamSync = !date;
    }

    const tsDelta = timestamp.subtract(this.#ref.ts);
    return this.#ref.time + tsDelta.valueOf();
  }

  _transform(chunk, _encoding, cb) {

    this.#transform(chunk).then(() => cb(), cb);
  }

  /**
   * Output chunk data split into packets of `packetSize` at the expected time.
   * 
   * @param {TimedChunk} chunk 
   */
  async #transform(chunk) {

    const outtime = this.#outputTime(chunk.start, chunk.date);
    if (outtime) {
      /** Positive => Too early, Negative => Too late! */
      const error = outtime - internals.accurateNow();
      let waittime = error - 2;      // Allow to be 2 ms early

      if (this.#streamSync) {
        if (error < -2_000 || error > 10_000) {
          this.emit('warning', new Error(`Excessive time offset: ${(error / 1000).toFixed(2)}s`));
          this.#ref = undefined;
          waittime = 0;
        }
      }
      else {

        // Drop mistimed chunks

        if (error < -this.limits.late) {
          this.dropped.late += chunk.buffers.length;
          return;
        }
        else if (error >= this.limits.future) {
          this.dropped.future += chunk.buffers.length;
          return;
        }
      }

      await internals.limitedWait(waittime);
    }

    let endtime = this.#outputTime(chunk.end);
    if (endtime) {
      const error = endtime - internals.accurateNow();

      if (error > 1000) {
        endtime = undefined;      // Immediately output and let next call to transform handle the error
      }
    }

    await this.#pushSmoothed(chunk.buffers, endtime);
  }

  async #pushSmoothed(buffers, endTime) {

    const bl = new BufferList(buffers);
    let index = 0;
    let accum = 0;

    while (index < bl.length) {
      const now = internals.accurateNow();
      this.push(bl.slice(index, index + this.packetSize));       // Ignore flow control
      index += this.packetSize;

      const remainingBytes = bl.length - index;
      if (remainingBytes > 0) {
        const remainingMs = endTime ? endTime - now : 0;
        const delay = (remainingBytes > this.packetSize ? remainingMs * (this.packetSize / remainingBytes) : remainingMs) + accum;

        if (delay >= 1) {
          await internals.limitedWait(delay);
          accum = 0;
        }
        else {
          accum += delay;
        }
      }
    }
  }
}
