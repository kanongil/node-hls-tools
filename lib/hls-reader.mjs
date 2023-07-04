import { PassThrough, Readable } from 'stream';
import { finished } from 'node:stream/promises';

import { assert } from 'hls-playlist-reader/helpers';

import { TsCollect } from './collect.mjs';
import { Smoother } from './smoother.mjs';
import SegmentDecrypt from './segment-decrypt.js';


export const HlsReader = class extends Readable {

  /** @type {TsCollect | undefined} */
  collector;

  /** @type {Smoother | undefined} */
  smoother;

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

    this.lowLatency = options.lowLatency ?? true;
    this.cookie = options.cookie;
    this.key = options.key;

    if (options.key && !Buffer.isBuffer(options.key) && options.key.length !== 32) {
      throw new TypeError('key must be a 32 byte Buffer');
    }

    this.isReading = false;
    this.isHooked = false;
    this.buffer = new PassThrough({ writableHighWaterMark: this.bufferSize, readableHighWaterMark: 0 });

    /** Nominal presentation delay for live playlists, in ms */
    this.presentDelay = undefined;

    this.reader.fetcher.source.index().then(async (/** @type {import("hls-playlist-reader/lib/fetcher").PlaylistObject} */ { index }) => {

      if (this.destroyed) {
        return;
      }

      assert(!index.master);

      if (index.isLive()) {
        this.presentDelay = index.target_duration * 3000;
        if (index.server_control) {
          const delaySecs = (this.lowLatency ? index.server_control.get('PART-HOLD-BACK', 'float') : 0) || index.server_control.get('HOLD-BACK', 'float');
          if (delaySecs) {
            this.presentDelay = delaySecs * 1000;
          }
        }
      }

      // Start output if not sync

      if (!this.sync) {
        this.hook();
      }

      // Start processing loop

      /** @type {ReadableStreamDefaultReader<import("hls-segment-reader").HlsStreamerObject>} */
      this._reader = this.reader.getReader();
      while (!this.destroyed) {

        // Note: this._reader cleanup is handled in _destroy()

        const { done, value } = await this._reader.read();
        if (done || this.destroyed) {
          break;
        }

        await this.process(value);
      }

      this.buffer.end();
    }).catch((err) => this.destroy(err));
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
        this.emit('warning', new Error('decrypt failed', { cause: err }));
        stream = segmentInfo.stream;
      }

      this.emit('segment', segmentInfo);

      const programTime = +segmentInfo.segment.entry.program_time;
      const startTime = (programTime && this.presentDelay) ? programTime + this.presentDelay : undefined;

      if (!this.isHooked) {
        if (startTime) {
          this.hook(startTime);
        }
        else {

          // Pull data and detect if we need to hook before end

          let buffered = 0;
          const checkHook = (chunk) => {

            buffered += chunk.length;
            if (this.isHooked || buffered >= this.bufferSize) {
              this.removeListener('data', checkHook);
              this.hook(startTime);
            }
          };

          stream.on('data', checkHook);
        }
      }

      stream.pipe(this.buffer, { end: false });

      try {
        await finished(stream);
        this.hook(startTime);
      }
      catch (err) {
        if (err.name !== 'AbortError') {
          this.emit('warning', new Error('stream error', { cause: err }));
        }
      }
    }
    finally {
      this.isReading = false;
    }
  }

  hook(startTime) {                          // the hook is used to prebuffer

    try {
      if (this.isHooked) return;

      this.isHooked = true;

      let s = this.buffer;
      if (this.sync) {
        this.collector = new TsCollect({ startTime });

        const smoother = this.smoother = new Smoother({ maxPresentationDelay: this.presentDelay ? this.presentDelay + 1000 : undefined });
        smoother.on('unpipe', () => this.unpipe());

        s = s.pipe(this.collector).pipe(smoother);
      }

      this.#pump(s)
        .then(() => this.push(null), (err) => this.destroy(err));

      this.emit('ready');
    }
    catch (err) {
      this.destroy(err);
    }
  }

  /** Pump src chunks into self */
  async #pump(src) {

    src.on('data', (chunk) => {

      if (!this.push(chunk)) {
        src.pause();
      }
    });

    this._read = () => src.resume();
    try {
      await finished(src);
    }
    finally {
      this._read = HlsReader.prototype._read;
    }
  }

  decrypted(stream, keyAttrs) {

    return SegmentDecrypt.decrypt(stream, keyAttrs, { base: this.reader.fetcher.source.baseUrl, key: this.key, cookie: this.cookie });
  }
};


export default function hlsreader(segmentReader, options) {

  return new HlsReader(segmentReader, options);
};
