var util = require('util'),
    assert = require('assert');

var async = require('async'),
    Transform = require('readable-stream/transform');

// TODO: ratelimit the individual bytes

module.exports = tslimit;
exports.TsLimit = TsLimit;

function parsePCR(packet) {
  var head = packet.readUInt32BE(0, true);
//  var b = packet.readUInt8(3, true);
  var pid = (head >> 8) & 0x1fff;
  if (((head >> 5) & 1) !== 1) return -1;

  var s = packet.readUInt8(4, true);
  if (s < 7) return -1;

  var f = packet.readUInt8(5, true);
  if (((f >> 4) & 1) !== 1) return -1;

  var base = packet.readUInt32BE(6, true) * 2;
  var ext = packet.readUInt32BE(10, true);

  base += (ext >> 31);
  ext = ext & 0x1ff;

  return base / 0.09 + ext / 27; // return usecs
}

function TsLimit() {
  var self = this;
  Transform.call(this);

  // the buffer is only used for partial TS packets ()< 188 bytes)
  this.buffer = new Buffer(0);

  this.pcr = -1;
  this.last = null;

  this.bitrate = 10E06;
  this.time = null;

  this.pcr2time = function(pcr) {
    if (self.pcr === -1) {
      self.pcr = pcr;
      self.last = utime();
    }

    var pcr_delta = pcr - self.pcr;
    if (pcr_delta < 0) pcr_delta += (0x200000000 * 300) / 27;

    return self.last + pcr_delta;
  }

  this.bytes2delta = function(bytes) {
    return bytes*8*1E6 / self.bitrate; /* useconds */
  }

  return this;
}
util.inherits(TsLimit, Transform);

function utime() {
  var t = process.hrtime(); // based on CLOCK_MONOTONIC, and thus accommodates local drift (but apparently not suspend)
  return t[0] * 1E6 + t[1] / 1E3;
}

TsLimit.prototype._transform = function(chunk, output, cb) {
  var self = this;
  this.buffer = Buffer.concat([this.buffer, chunk]);

  function process() {
    var buf = self.buffer;
    var end = buf.length-188-1;
    var now = utime();
    if (!self.time) self.time = now;

    var packet_time = self.bytes2delta(188);
    for (var i=0; i<end; i+=188) {
      self.time += packet_time;
      var pcr = parsePCR(buf.slice(i, i+188));
      if (pcr !== -1)
        self.time = self.pcr2time(pcr);
      if (self.time > now)
        break;
    }

    // TODO: limit output speed
    if (i) output(buf.slice(0, i));
    self.buffer = buf.slice(i);

    if (i < end) {
      return setTimeout(process, (self.time - now) / 1000);
    }
    cb();
  }

  process();
};

TsLimit.prototype._flush = function(output, cb) {
  if (this.buffer.length) output(this.buffer);
  cb();
};

function tslimit() {
  return new TsLimit();
}