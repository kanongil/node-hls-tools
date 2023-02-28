'use strict';

const debug = require('debug')('hls:tssmooth');

const { Transform } = require('stream');


const internals = {};


// In Transport Streams the intended rate is determined by the values of the PCR fields and the number of Transport Stream bytes between them. (ISO-13818-1 D.0.9)

const RateError = class extends Error {

  constructor(msg) {

    super();

    this.message = msg;
  }
};
RateError.prototype.name = 'Rate Error';


internals.parsePCR = function(buffer, index, pcr_pid) {

  let head = buffer.readUInt32BE(index, true);
  let pid = (head >> 8) & 0x1fff;
  if (((head >> 5) & 1) !== 1) return -1;
  if (pcr_pid && pcr_pid != pid) return -1;

  let s = buffer.readUInt8(index + 4, true);
  if (s < 7) return -1;

  let f = buffer.readUInt8(index + 5, true);
  if (((f >> 4) & 1) !== 1) return -1;

  let base = buffer.readUInt32BE(index + 6, true) * 2;
  let ext = buffer.readUInt32BE(index + 10, true);

  base += (ext >> 31);
  ext = ext & 0x1ff;

  return base / 0.09 + ext / 27; // return usecs
};


internals.utime = function() {

  let t = process.hrtime(); // based on CLOCK_MONOTONIC, and thus accommodates local drift (but apparently not suspend)
//  console.error(t);
  return t[0] * 1E6 + t[1] / 1E3;
};


internals.wait = function(waitMs, fn) {

  if (waitMs > 0)
    setTimeout(fn, Math.round(waitMs));
  else
    fn();
}

// Smoothly outputs given buffer before endTime

internals.outputBefore = function (stream, buffer, endTime, packetSize, cb) {

  let index = 0;

  const outputPacket = () => {

    let now = internals.utime();
    let packetTime = (endTime - now) * (packetSize / (buffer.length - index));

    stream.push(buffer.slice(index, Math.min(buffer.length, index + packetSize)));
    index += packetSize;

    let done = (index < buffer.length) ? outputPacket : cb;
    let delay = Math.round(Math.min(Math.max((0.8 * packetTime / 1000) - 1, 1), 50));
    if (delay === 1)
      process.nextTick(done);
    else
      setTimeout(done, delay);
  }

  outputPacket();
};

const TsSmooth = class extends Transform {

  constructor(options) {

    options = options || {};

    const packetSize = options.packetSize || 7 * 188;

    super({ highWaterMark: packetSize });

    this.packetSize = packetSize; // size of output packets
    this.startTime = options.startTime;

    this.buffer = Buffer.alloc(0);

    this.pcr = -1;
    this.last = null;

    this.lateLimit = 40_000; /* 40 ms */
    this.lateDropped = 0;
    //this.bitrate = 10E06;
    this.pcrTime = -1;

    this.pcrDelta = (pcr, lastPcr) => {

      let pcrDelta = pcr - lastPcr;
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

      let pcrDelta = this.pcrDelta(pcr, this.pcr);
      let ret = this.last + pcrDelta;
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

      let pcrtime = this.pcr2time(newPCR);
      if (this.pcrTime === -1) {
        this.pcrTime = pcrtime;
        return undefined;
      }

      let delta = pcrtime - this.pcrTime;
      this.pcrTime = pcrtime;

      return { time:pcrtime, delta: delta };
    };
  }

  reset(currentPCR) {

    this.pcr = -1;
    if (typeof currentPCR !== 'undefined')
      this.pcrTime = this.pcr2time(currentPCR);
  }

  _transform(chunk, encoding, cb) {

    let index = Math.floor(this.buffer.length / 188) * 188;
    this.buffer = Buffer.concat([this.buffer, chunk]);

    let buf = this.buffer;
    let end = buf.length - 188;

    let startIndex = 0;

    const processNext = () => {

      while (index < end) {
        // check sync
        if (buf.readUInt8(index + 188, true) !== 0x47) {
          // find next potential sync point
          debug('ts sync lost');
          let sync = index + 1;
          for (; sync < end; sync++) {
            if (buf.readUInt8(sync, true) === 0x47)
              break;
          }
          // remove bad data
          debug('slice', sync, end);
          buf = Buffer.concat([buf.slice(0, index), buf.slice(sync)]);
          end -= sync - index;
          continue;
        }

        let pcr = internals.parsePCR(buf, index);
        let out = this.outputTime(pcr);
        if (out !== undefined && index !== startIndex) {
          if (out.delta > 100E3 || out.delta < 0)
            this.emit('warning', new Error('PCR_error: ' + (out.delta / 1E6).toFixed(2) + 's missing'));

          let now = internals.utime();
          let error = (out.time - now) - out.delta;
          let waittime = (error > this.lateLimit) ? (error / 1000 - 5) : 0;

          if (!this.startTime && error < -2 * 1E6 || error > 300 * 1E6) {
            // negative == buffer too late
            // positive == buffer too early
            this.emit('warning', new RateError('PCR sync offset ' + (error / 1E6).toFixed(2) + 's error'));
            this.reset(pcr);
            waittime = 0;
          } else if (error < -this.lateLimit) {
            // ignore the data since it is too late
            this.lateDropped += index - startIndex;
            startIndex = index;
            return setImmediate(processNext);
          }

          let slice = buf.slice(startIndex, index);
          startIndex = index;
          /* eslint-disable no-loop-func */
          return internals.wait(waittime, () => {

            return internals.outputBefore(this, slice, out.time, this.packetSize, processNext);
          });
          /* eslint-enable */
        }
        index += 188;
      }

      if (startIndex !== 0) this.buffer = buf.slice(startIndex);
      cb();
    }

    processNext();
  }

  _flush(cb) {

    if (this.buffer.length) this.push(this.buffer); // TODO: use outputBefore() based on current stream speed?
    cb();
  }
};


const tssmooth = module.exports = function tssmooth(options) {

  return new TsSmooth(options);
};

tssmooth.TsSmooth = TsSmooth;
