'use strict';

const Util = require('util');
const Readable = require('readable-stream/readable');
const Passthrough = require('readable-stream/passthrough');

const StreamEach = require('stream-each');
const Oncemore = require('oncemore');

const tssmooth = require('./tssmooth');
const SegmentDecrypt = require('./segment-decrypt');


const internals = {
  NOOP: function(){},
};


// 'pipe' stream to a Readable
internals.pump = function(src, dst, done) {

  src.on('data', (chunk) => {

    if (!dst.push(chunk)) {
      src.pause();
    }
  });
  Oncemore(src).once('end', 'error', (err) => {

    // TODO: flush source buffer on error?
    dst._read = internals.NOOP;
    done(err);
  });
  dst._read = () => {
    src.resume();
  };
}

// TODO: use pipe as interface to segment-reader?

function HlsReader(segmentReader, options) {

  if (!(this instanceof HlsReader)) {
    return new HlsReader(segmentReader, options);
  }

  options = options || {};

  Readable.call(this, { lowWaterMark: options.lowWaterMark, highWaterMark: options.highWaterMark });

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

  StreamEach(this.reader, (segmentInfo, done) => {

    this.isReading = true;

    return this.decrypt(segmentInfo.stream, segmentInfo.details.keys, (err, stream) => {

      if (err) {
        console.error('decrypt failed', err.stack);
        stream = segmentInfo.stream;
      }

      this.emit('segment', segmentInfo);

      stream = Oncemore(stream);

      if (!this.isHooked) {
        // pull data and detect if we need to hook before end
        let buffered = 0;
        stream.on('data', (chunk) => {

          buffered += chunk.length;
          if (!this.isHooked && buffered >= this.bufferSize)
            this.hook();
        });
      }

      stream.pipe(this.buffer, { end: false });
      stream.once('end', 'error', (err) => {

        this.isReading = false;
        if (err) {
          console.error('stream error', err.stack || err);
        }
        this.hook();
        done();
      });
    });
  }, (err) => {

    if (err) throw err;

    this.buffer.end();
  });

  // start output if needed
  if (!this.sync) {
    process.nextTick(() => {

      this.hook();
    });
  }
}
Util.inherits(HlsReader, Readable);

HlsReader.prototype._read = internals.NOOP;

HlsReader.prototype.destroy = function () {
  
};

// the hook is used to prebuffer
HlsReader.prototype.hook = function hook() {

  if (this.isHooked) return;

  this.isHooked = true;

  let s = this.buffer;
  if (this.sync) {
    let smooth = tssmooth();
    smooth.on('unpipe', () => {

      this.unpipe();
    });
    smooth.on('warning', (err) => {

      console.error('smoothing error', err);
    });
    s = s.pipe(smooth);
  }

  internals.pump(s, this, (err) => {

    if (err) {
      return this.emit('error', err);
    }
    this.push(null);
  });

  this.emit('ready');
};

HlsReader.prototype.decrypt = function (stream, keyAttrs, next) {

  return SegmentDecrypt.decrypt(stream, keyAttrs, { base: this.reader.baseUrl, key: this.key, cookie: this.cookie }, next);
};


const hlsreader = module.exports = function hlsreader(segmentReader, options) {

  return new HlsReader(segmentReader, options);
};

hlsreader.HlsReader = HlsReader;
