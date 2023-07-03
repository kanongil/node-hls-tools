import { PassThrough, Readable } from 'stream';
import { finished } from 'node:stream/promises';

import Oncemore from 'oncemore';

import { TsCollect } from './collect.mjs';
import { Smoother } from './smoother.mjs';
import SegmentDecrypt from './segment-decrypt.js';


const internals = {
  NOOP: function() {},
};


// 'pipe' stream to a Readable
internals.pump = function (src, dst, done) {

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
};

internals.onWarning = function (err) {

  console.error('smoothing error', err);
};

export const HlsReader = class extends Readable {

  /**
   * @param {import("hls-segment-reader").HlsSegmentStreamer} segmentReader 
   * @param {*} options 
   */
  constructor(segmentReader, options) {

    options = options || {};

    super({ lowWaterMark: options.lowWaterMark, highWaterMark: options.highWaterMark, signal: options.signal });

    this.reader = segmentReader;
    this._reader = undefined;

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

      if (this.destroyed) {
        return;
      }

      this.presentDelay = index.target_duration * 3;
      if (index.server_control) {
        this.presentDelay = index.server_control.get('PART-HOLD-BACK', 'float') || index.server_control.get('HOLD-BACK', 'float') || this.presentDelay;
      }

      this._reader = this.reader.getReader();
      while (!this.destroyed) {
        const { done, value } = await this._reader.read();
        if (done || this.destroyed) {
          break;
        }

        await this.process(value);
      }

      this.buffer.end();
    }).catch((err) => this.emit('error', err));

    // start output if needed
    if (!this.sync) {
      process.nextTick(() => {

        this.hook();
      });
    }
  }

  _read() { };

  _destroy(err, cb) {

    (this._reader ?? this.reader).cancel(err);
    return super._destroy(err, cb);
  }

  /**
   * @param {import("hls-segment-reader").HlsStreamerObject} segmentInfo
   */
  async process(segmentInfo) {

    try {
      this.isReading = true;

      let stream;
      try {
        stream = await this.decrypted(segmentInfo.stream, segmentInfo.segment && segmentInfo.segment.entry.keys);
      }
      catch (err) {
        console.error('decrypt failed', err.stack);
        stream = segmentInfo.stream;
      }

      this.emit('segment', segmentInfo);

      const startTime = +segmentInfo.segment.entry.program_time + this.presentDelay * 1000;

      if (!this.isHooked) {

        // Pull data and detect if we need to hook before end

        let buffered = 0;
        stream.on('data', (chunk) => {

          try {
            buffered += chunk.length;
            if (!this.isHooked && buffered >= this.bufferSize) {
              this.hook(startTime);
            }
          }
          catch (err) {
            stream.destroy(err);
          }
        });
      }

      stream.pipe(this.buffer, { end: false });

      try {
        await finished(stream);
        this.hook(startTime);
      }
      catch (err) {
        if (err.name !== 'AbortError') {
          console.error('stream error', err);
        }
      }
    }
    catch (err) {
      console.error('process error', err);
      throw err;
    }
    finally {
      this.isReading = false;
    }
  }

  hook(startTime) {                          // the hook is used to prebuffer

    if (this.isHooked) return;

    this.isHooked = true;

    let s = this.buffer;
    if (this.sync) {
      const collector = new TsCollect({ startTime });
      collector.on('warning', internals.onWarning);

      const smooth = new Smoother();
      smooth.on('unpipe', () => this.unpipe());
      smooth.on('warning', internals.onWarning);
      s = s.pipe(collector).pipe(smooth);

      let last = { late: 0, future: 0 };
      const reporter = setInterval(() => {

        if (smooth.dropped.late !== last.late ||
            smooth.dropped.future !== last.future) {

          const delta = {
            late: smooth.dropped.late - last.late,
            future: smooth.dropped.future - last.future
          };

          console.error(`bytes dropped: ${JSON.stringify(delta)}`);
          last.late = smooth.dropped.late;
          last.future = smooth.dropped.future;
        }
      }, 1000);

      smooth.on('close', () => clearTimeout(reporter));
    }

    internals.pump(s, this, (err) => {

      if (err) {
        return this.emit('error', err);
      }
      this.push(null);
    });

    this.emit('ready');
  }

  decrypted(stream, keyAttrs) {

    return SegmentDecrypt.decrypt(stream, keyAttrs, { base: this.reader.fetcher.source.baseUrl, key: this.key, cookie: this.cookie });
  }
};


export default function hlsreader(segmentReader, options) {

  return new HlsReader(segmentReader, options);
};
