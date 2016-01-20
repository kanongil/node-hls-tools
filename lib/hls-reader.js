"use strict";

var Util = require('util');

var StreamProcess = require('streamprocess'),
    oncemore = require('oncemore');

var Readable = require('readable-stream/readable'),
    Passthrough = require('readable-stream/passthrough');

var tssmooth = require('./tssmooth');
var SegmentDecrypt = require('./segment-decrypt');


var internals = {
  NOOP: function(){},
};


// 'pipe' stream to a Readable
function pump(src, dst, done) {
  src.on('data', function(chunk) {
    if (!dst.push(chunk)) {
      src.pause();
    }
  });
  oncemore(src).once('end', 'error', function(err) {
    // TODO: flush source buffer on error?
    dst._read = internals.NOOP;
    done(err);
  });
  dst._read = function() {
    src.resume();
  };
}

// TODO: use pipe as interface to segment-reader?

function HlsReader(segmentReader, options) {
  if (!(this instanceof HlsReader))
    return new HlsReader(segmentReader, options);

  options = options || {};

  Readable.call(this, { lowWaterMark: options.lowWaterMark, highWaterMark: options.highWaterMark });

  var self = this;

  this.reader = segmentReader;

  this.sync = !!options.sync; // output in real-time
  this.bufferSize = ~~options.bufferSize;

  this.cookie = options.cookie;
  this.key = options.key;

  if (options.key && !Buffer.isBuffer(options.key) && options.key.length !== 32) {
    throw new TypeError('key must be a 32 byte Buffer');
  }

  this.isReading = false;
  this.isHooked = false;
  this.buffer = new Passthrough({ highWaterMark: this.bufferSize });

  StreamProcess(this.reader, function (segmentInfo, done) {
    self.isReading = true;

    return self.decrypt(segmentInfo.stream, segmentInfo.details.key, function (err, stream) {
      if (err) {
        console.error('decrypt failed', err.stack);
        stream = segmentInfo.stream;
      }

      self.emit('segment', segmentInfo);

      stream = oncemore(stream);

      if (!self.isHooked) {
        // pull data and detect if we need to hook before end
        var buffered = 0;
        stream.on('data', function(chunk) {
          buffered += chunk.length;
          if (!self.isHooked && buffered >= self.bufferSize)
            self.hook();
        });
      }

      stream.pipe(self.buffer, { end: false });
      stream.once('end', 'error', function(err) {
        self.isReading = false;
        if (err) {
          console.error('stream error', err.stack || err);
        }
        self.hook();
        done();
      });
    });
  });

  this.reader.on('end', function() {
    self.buffer.end();
  });

  // start output if needed
  if (!this.sync) {
    process.nextTick(function() {
      self.hook();
    });
  }
}
Util.inherits(HlsReader, Readable);

HlsReader.prototype._read = internals.NOOP;

HlsReader.prototype.destroy = function () {
  
};

// the hook is used to prebuffer
HlsReader.prototype.hook = function hook() {
  var self = this;
  if (this.isHooked) return;

  self.isHooked = true;

  var s = this.buffer;
  if (this.sync) {
    var smooth = tssmooth();
    smooth.on('unpipe', function() {
      this.unpipe();
    });
    smooth.on('warning', function(err) {
      console.error('smoothing error', err);
    });
    s = s.pipe(smooth);
  }

  pump(s, this, function(err) {
    if (err) {
      return self.emit('error', err);
    }
    self.push(null);
  });

  this.emit('ready');
};

HlsReader.prototype.decrypt = function (stream, keyAttrs, next) {
  return SegmentDecrypt.decrypt(stream, keyAttrs, { base: this.reader.baseUrl, key: this.key, cookie: this.cookie }, next);
};


var hlsreader = module.exports = function hlsreader(segmentReader, options) {
  return new HlsReader(segmentReader, options);
};

hlsreader.HlsReader = HlsReader;
