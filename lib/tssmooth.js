var util = require('util'),
    assert = require('assert'),
    debug = require('debug')('hls:tssmooth');

try {
  var Transform = require('stream').Transform;
  assert(Transform);
} catch (e) {
  var Transform = require('readable-stream/transform');
}

// In Transport Streams the intended rate is determined by the values of the PCR fields and the number of Transport Stream bytes between them. (ISO-13818-1 D.0.9)

module.exports = tssmooth;
exports.TsSmooth = TsSmooth;

function parsePCR(buffer, index, pcr_pid) {
  var head = buffer.readUInt32BE(index, true);
  var pid = (head >> 8) & 0x1fff;
  if (((head >> 5) & 1) !== 1) return -1;
  if (pcr_pid && pcr_pid != pid) return -1;

  var s = buffer.readUInt8(index+4, true);
  if (s < 7) return -1;

  var f = buffer.readUInt8(index+5, true);
  if (((f >> 4) & 1) !== 1) return -1;

  var base = buffer.readUInt32BE(index+6, true) * 2;
  var ext = buffer.readUInt32BE(index+10, true);

  base += (ext >> 31);
  ext = ext & 0x1ff;

  return base / 0.09 + ext / 27; // return usecs
}

function TsSmooth(options) {
  var self = this;
  options = options || {};

  this.packetSize = options.packetSize || 7*188; // size of output packets

  this.buffer = new Buffer(0);

  this.pcr = -1;
  this.last = null;

  this.bitrate = 10E06;
  this.pcrtime = -1;

  this.pcrdelta = function(pcr, pcr_old) {
    var pcr_delta = pcr - pcr_old;
    if (pcr_delta < 0) pcr_delta += (0x200000000 * 300) / 27;
    return pcr_delta;
  }

  this.pcr2time = function(pcr) {
    if (self.pcr === -1) {
      self.pcr = pcr;
      self.last = utime();
    }

    var pcr_delta = self.pcrdelta(pcr, self.pcr);
    var ret = self.last + pcr_delta;
    if (pcr_delta > 3600E6) {
      // update pcr reference every hour to handle wrap-around
      self.pcr = pcr;
      self.last = ret;
    }
    return ret;
  }

  this.output_time = function(newPCR) {
    if (newPCR === -1) return -1;

    var pcrtime = self.pcr2time(newPCR);
    if (self.pcrtime === -1) {
      self.pcrtime = pcrtime;
      return -1;
    }

    var delta = pcrtime - self.pcrtime;
    if (delta > 100E3 || delta < 0) {
      console.error('PCR_error: '+(delta/1E6).toFixed(2)+'s missing');
      var now = utime();
      var error = now - pcrtime;
      if (Math.abs(error) > 60*1E6) {
        console.error('PCR sync reset from '+(error/1E6).toFixed(2)+'s error');
        self.pcr = -1;
        pcrtime = self.pcr2time(newPCR);
      }
    }
    self.pcrtime = pcrtime;
    return pcrtime;
  }

  Transform.call(this);
//  Transform.call(this, {bufferSize:5*this.packetSize, highWaterMark:5*this.packetSize});
//  Transform.call(this, {bufferSize:188*7, highWaterMark:64*1024});
}
util.inherits(TsSmooth, Transform);

function utime() {
  var t = process.hrtime(); // based on CLOCK_MONOTONIC, and thus accommodates local drift (but apparently not suspend)
//  console.error(t);
  return t[0] * 1E6 + t[1] / 1E3;
}

// smoothly outputs given buffer before endTime
function outputBefore(stream, buffer, endTime, packetSize, cb) {
  var index = 0;

  function outputPacket() {
    var now = utime();
    var packetTime = (endTime - now) * (packetSize / (buffer.length - index));

    stream.push(buffer.slice(index, Math.min(buffer.length, index+packetSize)));
    index += packetSize;

    setTimeout((index < buffer.length) ? outputPacket: cb, Math.min(Math.max(0.95*packetTime/1000, 1), 50));
  }
  outputPacket();
}

TsSmooth.prototype._transform = function(chunk, encoding, cb) {
  var self = this;

  var index = Math.floor(this.buffer.length/188)*188;
  this.buffer = Buffer.concat([this.buffer, chunk]);

  var buf = self.buffer;
  var end = buf.length-188;

  var startIndex = 0;
  function processNext() {
    while (index < end) {
      // check sync
      if (buf.readUInt8(index+188, true) !== 0x47) {
        // find next potential sync point
        debug('ts sync lost');
        var sync = index+1;
        for (; sync < end; sync++) {
          if (buf.readUInt8(sync, true) === 0x47)
            break;
        }
        // remove bad data
        console.error('slice', sync, end);
        buf = Buffer.concat([buf.slice(0, index), buf.slice(sync)]);
        end -= sync-index;
        continue;
      }

      var pcr = parsePCR(buf, index);
      var outtime = self.output_time(pcr);
      if (outtime !== -1 && index !== startIndex) {
        var slice = buf.slice(startIndex, index);
        startIndex = index;
        return outputBefore(self, slice, outtime, self.packetSize, processNext);
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

function tssmooth(options) {
  return new TsSmooth(options);
}