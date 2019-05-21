'use strict';

const Path = require('path');
const Url = require('url');

const Bounce = require('@hapi/bounce');
const Mime = require('mime-types');
const StreamEach = require('stream-each');
const M3U8Parse = require('m3u8parse');
const debug = require('debug')('hls:recorder');

const HlsUploader = require('./hls-uploader');
const SegmentDecrypt = require('./segment-decrypt');


// Add custom extensions

Mime.extensions['audio/aac'] = ['aac'];
Mime.extensions['audio/ac3'] = ['ac3'];
Mime.extensions['video/iso.segment'] = ['m4s'];


const pathOrUriToURL = function (dirpathOrUri) {

  if (!dirpathOrUri.endsWith('/')) {
    dirpathOrUri = `${dirpathOrUri}/`;
  }

  const url = new URL(dirpathOrUri, 'r:/');
  return url.protocol === 'r:' ? Url.pathToFileURL(dirpathOrUri) : url;
};


const HlsStreamRecorder = class {

  constructor(reader, dst, options) {

    options = options || {};

    this.reader = reader;
    this.dst = pathOrUriToURL(dst); // target directory / s3 url

    this.nextSegmentSeq = -1;
    this.seq = 0;
    this.index = null;

    this.startOffset = parseFloat(options.startOffset);
    this.subreader = options.subreader;
    this.collect = !!options.collect; // collect into a single file (v4 feature)
    this.decrypt = options.decrypt;

    this.recorders = [];

    this.mapSeq = 0;
    this.nextMap = null;
    this.segmentExt = null;
  
    this.uploader = null;
    this.segmentHead = 0;

    this.ended = {};
    this.ended.promise = new Promise((resolve, reject) => {

      this.ended.resolve = resolve;
      this.ended.reject = reject;
    });

    this.reader.on('error', this.ended.reject);
  }

  start() {

    this.uploader = new HlsUploader(this.dst, { collect: this.collect });

    StreamEach(this.reader, this.process.bind(this));

    this.updateIndex(this.reader.index);
    this.reader.on('index', this.updateIndex.bind(this));
  }

  async completed() {

    await this.ended.promise;
    await Promise.all(this.recorders.map((r) => r.completed()));
  }

  updateIndex(update) {

    if (!update) {
      return;
    }

    if (!this.index) {
      this.index = new M3U8Parse.M3U8Playlist(update);
      if (!this.index.master) {
        if (this.collect)
          this.index.version = Math.max(4, this.index.version);  // v4 is required for byterange support
        this.index.version = Math.max(2, this.index.version);    // v2 is required to support the remapped IV attribute
        if (this.index.version !== update.version)
          debug('changed index version to:', this.index.version);

        this.index.segments = [];
        this.index.first_seq_no = this.seq;
        this.index.type = 'EVENT';
        this.index.ended = false;
        this.index.discontinuity_sequence = 0; // not allowed in event playlists

        if (!isNaN(this.startOffset)) {
          let offset = this.startOffset;
          if (!update.ended) {
            if (offset < 0) offset = Math.min(offset, -3 * this.index.target_duration);
          }

          this.index.start.signedDecimalFloatingPoint('time-offset', offset);
        }
      }
      else {
        debug('variants', this.index.variants);
        if (this.subreader) {

          // Remove backup sources

          let used = {};
          this.index.variants = this.index.variants.filter((variant) => {

            let bw = parseInt(variant.info.bandwidth, 10);
            let res = !(bw in used);
            used[bw] = true;
            return res;
          });

          const Recorder = this.constructor; // use subclassed HlsStreamRecorder class

          this.index.variants.forEach((variant, index) => {

            let variantUrl = Url.resolve(this.reader.baseUrl, variant.uri);
            debug('url', variantUrl);

            // check for duplicate source urls
            let rec = this.recorderForUrl(variantUrl);
            if (!rec || !rec.localUrl) {
              let dir = this.variantName(variant.info, index);
              rec = new Recorder(this.subreader(variantUrl), new URL(dir, this.dst).href, { startOffset: this.startOffset, collect: this.collect, decrypt: this.decrypt });
              rec.localUrl = Url.format({pathname: Path.join(dir, 'index.m3u8')});
              rec.remoteUrl = variantUrl;

              this.recorders.push(rec);
            }

            variant.uri = rec.localUrl;
          });

          const allGroups = [];
          for (const groupItems of this.index.groups.values())
            Array.prototype.push.apply(allGroups, groupItems);

          allGroups.forEach((groupItem, index) => {

            let srcUri = groupItem.quotedString('uri');
            if (srcUri) {
              let itemUrl = Url.resolve(this.reader.baseUrl, srcUri);
              debug('url', itemUrl);

              let rec = this.recorderForUrl(itemUrl);
              if (!rec || !rec.localUrl) {
                let dir = this.groupSrcName(groupItem, index);
                rec = new Recorder(this.subreader(itemUrl), new URL(dir, this.dst).href, { startOffset: this.startOffset, collect: this.collect, decrypt: this.decrypt });
                rec.localUrl = Url.format({pathname: Path.join(dir, 'index.m3u8')});
                rec.remoteUrl = itemUrl;

                this.recorders.push(rec);
              }

              groupItem.quotedString('uri', rec.localUrl);
            }
          });

          // Start all recordings

          this.recorders.forEach((recording) => {
            recording.start();
          });

          this.index.iframes = [];
        }
        else {
          this.index.variants = [];
          this.index.groups = new Map();
          this.index.iframes = [];
        }
      }

      // Hook end listener

      this.reader.on('end', async () => {

        this.index.ended = true;
        await this.flushIndex();
        this.ended.resolve();
        debug('done');
      });

      if (this.decrypt) {
        this.decrypt.base = this.reader.baseUrl;
      }
    }

    // Validate update

    if (this.index.target_duration > update.target_duration) {
      throw new Error('Invalid index');
    }
  }

  async process(segmentInfo, done) {

    let result;
    try {
      if (segmentInfo.type === 'segment') {
        return await this.processSegment(segmentInfo);
      }

      if (segmentInfo.type === 'init') {
        return await this.processInfo(segmentInfo);
      }

      debug('unknown segment type: ' + segmentInfo.type);
    }
    catch (err) {
      result = err;
    }
    finally {
      done(result);
    }
  }

  async processInfo(segmentInfo) {

    const meta = segmentInfo.file;
    const uri = `${this.segmentName(this.mapSeq, true)}.${Mime.extension(meta.mime)}`;

    this.mapSeq++;

    let bytesWritten = 0;
    try {
      bytesWritten = await this.writeStream(segmentInfo.stream, uri, meta);
    }
    catch (err) {
      Bounce.rethrow(err, 'system');
      // only to report errors
      debug('stream error', err.stack || err);
    }

    const map = new M3U8Parse.AttrList();

    map.quotedString('uri', uri);

    // handle byterange
    if (this.collect) {
      map.quotedString('byterange', `${bytesWritten}@${this.uploader.segmentBytes - bytesWritten}`);
      this.segmentExt = Mime.extension(meta.mime);
    }

    this.nextMap = map;
  }

  async processSegment(segmentInfo) {

    let segment = new M3U8Parse.M3U8Segment(segmentInfo.segment.details, true);
    let meta = segmentInfo.file;

    // mark discontinuities
    if (this.nextSegmentSeq !== -1 &&
        this.nextSegmentSeq !== segmentInfo.segment.seq) {
      segment.discontinuity = true;
    }
    this.nextSegmentSeq = segmentInfo.segment.seq + 1;

    // create our own uri
    segment.uri = `${this.segmentName(this.seq)}.${this.segmentExt || Mime.extension(meta.mime)}`;

    // add map info
    if (this.nextMap) {
      segment.map = this.nextMap;
      this.nextMap = null;
    }

    delete segment.byterange;

    // save the stream segment
    let stream;
    try {
      stream = await SegmentDecrypt.decrypt(segmentInfo.stream, segmentInfo.segment.details.keys, this.decrypt);
    }
    catch (err) {
      console.error('decrypt failed', err.stack);
      stream = segmentInfo.stream;
    }
  
    if (stream !== segmentInfo.stream) {
      segment.keys = null;
      meta = { mime: meta.mime, modified: meta.modified }; // size is no longer valid
    }

    this.seq++;

    let bytesWritten = 0;
    try {
      bytesWritten = await this.writeStream(stream, segment.uri, meta);
    }
    catch (err) {
      Bounce.rethrow(err, 'system');

        // only report errors
      debug('stream error', err.stack || err);
    }

    // handle byterange
    if (this.collect) {
      const isContigious = this.segmentHead > 0 && ((this.segmentHead + bytesWritten) === this.uploader.segmentBytes);
      segment.byterange = {
        length: bytesWritten,
        offset: isContigious ? null : this.uploader.segmentBytes - bytesWritten
      }

      this.segmentHead = this.uploader.segmentBytes;
    }

    // update index
    this.index.segments.push(segment);
    return this.flushIndex();
  }

  writeStream(stream, name, meta) {

    return this.uploader.pushSegment(stream, name, meta);
  }

  variantName(info, index) {

    return `v${index}`;
  }

  groupSrcName(info, index) {

    let lang = (info.quotedString('language') || '').replace(/\W/g, '').toLowerCase();
    let id = (info.quotedString('group-id') || 'unk').replace(/\W/g, '').toLowerCase();
    return `grp/${id}/${lang ? lang + '-' : ''}${index}`;
  }

  segmentName(seqNo, isInit) {

    const name = (n) => n;

    return this.collect ? 'stream' : (isInit ? 'init-' : '') + name(seqNo);
  }

  flushIndex() {

    return this.uploader.flushIndex(this.index);
  }

  recorderForUrl(remoteUrl) {

    let idx, len = this.recorders.length;
    for (idx = 0; idx < len; idx++) {
      let rec = this.recorders[idx];
      if (rec.remoteUrl === remoteUrl) {
        return rec;
      }
    }

    return null;
  }
};


const hlsrecorder = module.exports = function hlsrecorder(reader, dst, options) {

  return new HlsStreamRecorder(reader, dst, options);
};


hlsrecorder.HlsStreamRecorder = HlsStreamRecorder;
