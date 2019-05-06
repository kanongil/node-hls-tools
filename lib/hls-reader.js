'use strict';

const { PassThrough, Readable } = require('readable-stream');

const StreamEach = require('stream-each');
const Oncemore = require('oncemore');
const Pati = require('pati');

const TsSmooth = require('./tssmooth');
const SegmentDecrypt = require('./segment-decrypt');


const internals = {
  NOOP: function() {},
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

const HlsReader = class extends Readable {

  constructor(segmentReader, options) {

    options = options || {};

    super({ lowWaterMark: options.lowWaterMark, highWaterMark: options.highWaterMark });

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
    this.buffer = new PassThrough({ highWaterMark: this.bufferSize });

    const processStreams = () => {

      StreamEach(this.reader, this.process.bind(this), (err) => {

        if (err) {
          return processStreams(); // retry
        }

        this.buffer.end();
      });
    };

    if (this.reader.index) {
      processStreams();
    } else {
      this.reader.once('index', processStreams);
    }

    // start output if needed
    if (!this.sync) {
      process.nextTick(() => {

        this.hook();
      });
    }
  }

  _read() {};

  destroy() {

  }

  async process(segmentInfo, done) {

    let result;
    try {
      this.isReading = true;

      let stream;
      try {
        stream = await this.decrypt(segmentInfo.stream, segmentInfo.segment && segmentInfo.segment.details.keys);
      }
      catch (err) {
        console.error('decrypt failed', err.stack);
        stream = segmentInfo.stream;
      }

      this.emit('segment', segmentInfo);

      const dispatcher = new Pati.EventDispatcher(stream);
      dispatcher.on('end', Pati.EventDispatcher.end);

      if (!this.isHooked) {
        // pull data and detect if we need to hook before end
        let buffered = 0;
        dispatcher.on('data', (chunk) => {

          buffered += chunk.length;
          if (!this.isHooked && buffered >= this.bufferSize) {
            this.hook();
          }
        });
      }

      stream.pipe(this.buffer, { end: false });

      try {
        await dispatcher.finish();
      }
      catch (err) {
        console.error('stream error', err.stack || err);
      }

      this.isReading = false;
      this.hook();
    }
    catch (err) {
      console.error('process error', err.stack || err);
      result = err;
    }
    finally {
      done(result);
    }
  }

  hook() {                          // the hook is used to prebuffer

    if (this.isHooked) return;

    this.isHooked = true;

    let s = this.buffer;
    if (this.sync) {
      let smooth = TsSmooth();
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
  }

  decrypt(stream, keyAttrs) {

    return SegmentDecrypt.decrypt(stream, keyAttrs, { base: this.reader.baseUrl, key: this.key, cookie: this.cookie });
  }
};

const hlsreader = module.exports = function hlsreader(segmentReader, options) {

  return new HlsReader(segmentReader, options);
};

hlsreader.HlsReader = HlsReader;
