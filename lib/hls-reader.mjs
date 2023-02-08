import { PassThrough, Readable } from 'stream';

import Oncemore from 'oncemore';
import Pati from 'pati';

import TsSmooth from './tssmooth.js';
import SegmentDecrypt from './segment-decrypt.js';


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

export const HlsReader = class extends Readable {

  /**
   * @param {import("hls-segment-reader").HlsSegmentStreamer} segmentReader 
   * @param {*} options 
   */
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
    this.buffer = new PassThrough({ writableHighWaterMark: this.bufferSize, readableHighWaterMark: 0 });
    this.presentDelay = 0;

    this.reader.fetcher.source.index().then(async (/** @type {import("hls-playlist-reader/lib/fetcher").PlaylistObject} */ { index }) => {

      this.presentDelay = index.target_duration * 3;
      if (index.server_control) {
        this.presentDelay = index.server_control.get('PART-HOLD-BACK', 'float') || index.server_control.get('HOLD-BACK', 'float') || this.presentDelay;
      }

      try {
        const reader = this.reader.getReader();
        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            break;
          }

          await this.process(value);
        }

        this.buffer.end();
      }
      catch (err) {
        console.error('FATAL error', err);   // TODO: retry when appropriate
        //return processStreams(); // retry
      }
    });

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

  /**
   * @param {import("hls-segment-reader").HlsStreamerObject} segmentInfo
   */
  async process(segmentInfo) {

    try {
      this.isReading = true;

      let stream;
      try {
        stream = await this.decrypt(segmentInfo.stream, segmentInfo.segment && segmentInfo.segment.entry.keys);
      }
      catch (err) {
        console.error('decrypt failed', err.stack);
        stream = segmentInfo.stream;
      }

      this.emit('segment', segmentInfo);

      const dispatcher = new Pati.EventDispatcher(stream);
      dispatcher.on('end', Pati.EventDispatcher.end);

      const startTime = +segmentInfo.segment.entry.program_time + this.presentDelay * 1000;

      if (!this.isHooked) {
        // pull data and detect if we need to hook before end
        let buffered = 0;
        dispatcher.on('data', (chunk) => {

          buffered += chunk.length;
          if (!this.isHooked && buffered >= this.bufferSize) {
            this.hook(startTime);
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
      this.hook(startTime);
    }
    catch (err) {
      console.error('process error', err.stack || err);
      throw err;
    }
  }

  hook(startTime) {                          // the hook is used to prebuffer

    if (this.isHooked) return;

    this.isHooked = true;

    let s = this.buffer;
    if (this.sync) {
      let smooth = TsSmooth({ startTime });
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


export default function hlsreader(segmentReader, options) {

  return new HlsReader(segmentReader, options);
};
