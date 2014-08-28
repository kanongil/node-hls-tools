"use strict";

var util = require('util'),
    assert = require('assert'),
    debug = require('debug')('hls:tssmooth');

var Transform = require('readable-stream/transform');

// In Transport Streams the intended rate is determined by the values of the PCR fields and the number of Transport Stream bytes between them. (ISO-13818-1 D.0.9)

function RateError(msg) {
  Error.call(this);

  this.message = msg;
}
util.inherits(RateError, Error);
RateError.prototype.name = 'Rate Error';

function parsePCR(buffer, index, pcr_pid) {
  var head = buffer.readUInt32BE(index, true);
  var pid = (head >> 8) & 0x1fff;
  if (((head >> 5) & 1) !== 1) return -1;
  if (pcr_pid && pcr_pid != pid) return -1;

  var s = buffer.readUInt8(index + 4, true);
  if (s < 7) return -1;

  var f = buffer.readUInt8(index + 5, true);
  if (((f >> 4) & 1) !== 1) return -1;

  var base = buffer.readUInt32BE(index + 6, true) * 2;
  var ext = buffer.readUInt32BE(index + 10, true);

  base += (ext >> 31);
  ext = ext & 0x1ff;

  return base / 0.09 + ext / 27; // return usecs
}

function utime() {
  var t = process.hrtime(); // based on CLOCK_MONOTONIC, and thus accommodates local drift (but apparently not suspend)
//  console.error(t);
  return t[0] * 1E6 + t[1] / 1E3;
}

function wait(waitMs, fn) {
  if (waitMs > 0)
    setTimeout(fn, Math.round(waitMs));
  else
    fn();
}

function TsSmooth(options) {
  var self = this;
  options = options || {};

  this.packetSize = options.packetSize || 7 * 188; // size of output packets

  this.buffer = new Buffer(0);

  this.pcr = -1;
  this.last = null;

  this.errorLimit = 80000; /* 80 ms */
  this.bitrate = 10E06;
  this.pcrTime = -1;

  this.pcrDelta = function(pcr, lastPcr) {
    var pcrDelta = pcr - lastPcr;
    if (pcrDelta < 0) pcrDelta += (0x200000000 * 300) / 27;
    return pcrDelta;
  };

  this.pcr2time = function(pcr) {
    if (self.pcr === -1) {
      self.pcr = pcr;
      self.last = utime();
    }

    var pcrDelta = self.pcrDelta(pcr, self.pcr);
    var ret = self.last + pcrDelta;
    if (pcrDelta > 3600E6) {
      // update pcr reference every hour to handle wrap-around
      self.pcr = pcr;
      self.last = ret;
    }
    return ret;
  };

  this.outputTime = function(newPCR) {
    // when this is called normally, now ~= self.pcrtime
    if (newPCR === -1) return undefined;

    var pcrtime = self.pcr2time(newPCR);
    if (self.pcrTime === -1) {
      self.pcrTime = pcrtime;
      return undefined;
    }

    var delta = pcrtime - self.pcrTime;
    self.pcrTime = pcrtime;

    return { time:pcrtime, delta: delta };
  };

  Transform.call(this, {highWaterMark:this.packetSize});
}
util.inherits(TsSmooth, Transform);

TsSmooth.prototype.reset = function(currentPCR) {
  this.pcr = -1;
  if (typeof currentPCR !== 'undefined')
    this.pcrTime = this.pcr2time(currentPCR);
};

// smoothly outputs given buffer before endTime
function outputBefore(stream, buffer, endTime, packetSize, cb) {
  var index = 0;

  function outputPacket() {
    var now = utime();
    var packetTime = (endTime - now) * (packetSize / (buffer.length - index));

    stream.push(buffer.slice(index, Math.min(buffer.length, index + packetSize)));
    index += packetSize;

    var done = (index < buffer.length) ? outputPacket : cb;
    var delay = Math.round(Math.min(Math.max((0.8 * packetTime / 1000) - 1, 1), 50));
    if (delay === 1)
      process.nextTick(done);
    else
      setTimeout(done, delay);
  }
  outputPacket();
}

TsSmooth.prototype._transform = function(chunk, encoding, cb) {
  var self = this;

  var index = Math.floor(this.buffer.length / 188) * 188;
  this.buffer = Buffer.concat([this.buffer, chunk]);

  var buf = self.buffer;
  var end = buf.length - 188;

  var startIndex = 0;
  function processNext() {
    while (index < end) {
      // check sync
      if (buf.readUInt8(index + 188, true) !== 0x47) {
        // find next potential sync point
        debug('ts sync lost');
        var sync = index + 1;
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

      var pcr = parsePCR(buf, index);
      var out = self.outputTime(pcr);
      if (out !== undefined && index !== startIndex) {
        if (out.delta > 100E3 || out.delta < 0)
          self.emit('warning', new Error('PCR_error: ' + (out.delta / 1E6).toFixed(2) + 's missing'));

        var now = utime();
        var error = (out.time - now) - out.delta;
        var waittime = (error > self.errorLimit) ? (error / 1000 - 5) : 0;

        if (error < -2 * 1E6 || error > 300 * 1E6) {
          // negative == buffer too late
          // positive == buffer too early
          self.emit('warning', new RateError('PCR sync offset ' + (error / 1E6).toFixed(2) + 's error'));
          self.reset(pcr);
          waittime = 0;
        } else if (error < -self.errorLimit) {
          // ignore the data since it is too late
          return setImmediate(processNext);
        }

        var slice = buf.slice(startIndex, index);
        startIndex = index;
        /* eslint-disable no-loop-func */
        return wait(waittime, function output() {
          return outputBefore(self, slice, out.time, self.packetSize, processNext);
        });
        /* eslint-enable */
      }
      index += 188;
    }

    if (startIndex !== 0) self.buffer = buf.slice(startIndex);
    cb();
  }

  processNext();
};

TsSmooth.prototype._flush = function(cb) {
  if (this.buffer.length) this.push(this.buffer); // TODO: use outputBefore() based on current stream speed?
  cb();
};


var tssmooth = module.exports = function tssmooth(options) {
  return new TsSmooth(options);
};

tssmooth.TsSmooth = TsSmooth;
