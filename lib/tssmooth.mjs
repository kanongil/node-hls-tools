import { Transform } from 'stream';
import { setTimeout as setTimeoutP } from 'timers/promises';

import { BufferList } from 'bl';

import { TimedChunk } from './collect.mjs';


const internals = {};


// In Transport Streams the intended rate is determined by the values of the PCR fields and the number of Transport Stream bytes between them. (ISO-13818-1 D.0.9)

const RateError = class extends Error {

  constructor(msg) {

    super();

    this.message = msg;
  }
};
RateError.prototype.name = 'Rate Error';



internals.utime = function() {

  const t = process.hrtime(); // based on CLOCK_MONOTONIC, and thus accommodates local drift (but apparently not suspend)
//  console.error(t);
  return t[0] * 1E6 + t[1] / 1E3;
};


internals.wait = function(waitMs) {

  if (waitMs > 0)
    return setTimeoutP(Math.round(waitMs));
}


export class TsSmooth extends Transform {

  packetSize;
  startTime;

  pcr = -1;
  last = null;
  lateLimit = 40_000; /* 40 ms */
  lateDropped = 0;
  //bitrate = 10E06;
  pcrTime = -1;

  constructor(options) {

    options = options || {};

    const packetSize = options.packetSize || 7 * 188;

    super({
      writableObjectMode: true,
      writableHighWaterMark: 1
    });

    this.packetSize = packetSize; // size of output packets
    this.startTime = options.startTime;

    this.pcrDelta = (pcr, lastPcr) => {

      const pcrDelta = pcr - lastPcr;
      if (pcrDelta < 0) pcrDelta += (0x200000000 * 300) / 27;
      return pcrDelta;
    };

    this.pcr2time = (pcr) => {

      if (this.pcr === -1) {
        this.pcr = pcr;
        this.last = internals.utime();
        if (this.startTime) {
          const delay = this.startTime - Date.now();
          this.last += delay * 1000;
        }
      }

      const pcrDelta = this.pcrDelta(pcr, this.pcr);
      const ret = this.last + pcrDelta;
      if (pcrDelta > 3600E6) {

        // Update pcr reference every hour to handle wrap-around

        this.pcr = pcr;
        this.last = ret;
      }

      return ret;
    };

    this.outputTime = (newPCR) => {

      // when this is called normally, now ~= this.pcrtime
      if (newPCR === -1) return undefined;

      const pcrtime = this.pcr2time(newPCR);
      if (this.pcrTime === -1) {
        this.pcrTime = pcrtime;
        return undefined;
      }

      const delta = pcrtime - this.pcrTime;
      this.pcrTime = pcrtime;

      return { time:pcrtime, delta };
    };
  }

  reset(currentPCR) {

    this.pcr = -1;
    if (typeof currentPCR !== 'undefined')
      this.pcrTime = this.pcr2time(currentPCR);
  }

  _transform(chunk, _encoding, cb) {

    this.#transform(chunk).then(() => cb(), cb);
  }

  /**
   * @param {TimedChunk} chunk 
   */
  async #transform(chunk) {

    const out = this.outputTime(chunk.end);
    if (out === undefined) {
      for (const buf of chunk.buffers) {
        this.push(buf);
      }

      return;
    }

    if (out.delta > 100E3 || out.delta < 0)
      this.emit('warning', new Error('PCR_error: ' + (out.delta / 1E6).toFixed(2) + 's missing'));

    const now = internals.utime();
    const error = (out.time - now) - out.delta;
    let waittime = (error > this.lateLimit) ? (error / 1000 - 5) : 0;

    if (!this.startTime && error < -2 * 1E6 || error > 300 * 1E6) {
      // negative == chunk too late
      // positive == chunk too early
      this.emit('warning', new RateError('PCR sync offset ' + (error / 1E6).toFixed(2) + 's error'));
      this.reset(chunk.pcr);
      waittime = 0;
    }
    else if (error < -this.lateLimit) {

      // Ignore the chunk since it is too late

      this.lateDropped += chunk.size;
      return;
    }

    await internals.wait(waittime);

    await this.#pushSmoothed(chunk.buffers, out.time);
  }

  async #pushSmoothed(buffers, endTime) {

    const bl = new BufferList(buffers);
    let index = 0;

    while (index < bl.length) {
      const now = internals.utime();
      this.push(bl.slice(index, index + this.packetSize));       // Ignore flow control
      index += this.packetSize;

      const packetTime = (endTime - now) * (this.packetSize / (bl.length - index));
      const delay = Math.round(Math.min(Math.max((0.8 * packetTime / 1000) - 1, 1), 10));

      if (delay > 1) {
        await setTimeoutP(delay);
      }
    }
  }
}
