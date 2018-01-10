'use strict';

const Path = require('path');
const Url = require('url');

const Mime = require('mime-types');
const StreamEach = require('stream-each');
const M3U8Parse = require('m3u8parse');
const debug = require('debug')('hls:recorder');

const HlsUploader = require('./hls-uploader');
const SegmentDecrypt = require('./segment-decrypt');


// add custom extensions
Mime.extensions['audio/aac'] = ['aac'];
Mime.extensions['audio/ac3'] = ['ac3'];


function HlsStreamRecorder(reader, dst, options) {

  options = options || {};

  this.reader = reader;
  this.dst = dst; // target directory

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

  this.uploader = null;
  this.segmentHead = 0;
}

HlsStreamRecorder.prototype.start = function() {

  this.uploader = new HlsUploader(this.dst, { collect: this.collect });

  StreamEach(this.reader, this.process.bind(this));

  this.updateIndex(this.reader.index);
  this.reader.on('index', this.updateIndex.bind(this));
};

HlsStreamRecorder.prototype.updateIndex = function(update) {

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
        this.index.start.decimalInteger('time-offset', offset);
      }
    }
    else {
      debug('variants', this.index.variants);
      if (this.subreader) {
        // remove backup sources
        let used = {};
        this.index.variants = this.index.variants.filter((variant) => {

          let bw = parseInt(variant.info.bandwidth, 10);
          let res = !(bw in used);
          used[bw] = true;
          return res;
        });

        this.index.variants.forEach((variant, index) => {

          let variantUrl = Url.resolve(this.reader.baseUrl, variant.uri);
          debug('url', variantUrl);

          // check for duplicate source urls
          let rec = this.recorderForUrl(variantUrl);
          if (!rec || !rec.localUrl) {
            let dir = this.variantName(variant.info, index);
            rec = new HlsStreamRecorder(this.subreader(variantUrl), Path.join(this.dst, dir), { startOffset: this.startOffset, collect: this.collect, decrypt: this.decrypt });
            rec.localUrl = Url.format({pathname: Path.join(dir, 'index.m3u8')});
            rec.remoteUrl = variantUrl;

            this.recorders.push(rec);
          }

          variant.uri = rec.localUrl;
        });

        let allGroups = [];
        for (let group in this.index.groups)
          Array.prototype.push.apply(allGroups, this.index.groups[group]);

        allGroups.forEach((groupItem, index) => {

          let srcUri = groupItem.quotedString('uri');
          if (srcUri) {
            let itemUrl = Url.resolve(this.reader.baseUrl, srcUri);
            debug('url', itemUrl);

            let rec = this.recorderForUrl(itemUrl);
            if (!rec || !rec.localUrl) {
              let dir = this.groupSrcName(groupItem, index);
              rec = new HlsStreamRecorder(this.subreader(itemUrl), Path.join(this.dst, dir), { startOffset: this.startOffset, collect: this.collect, decrypt: this.decrypt });
              rec.localUrl = Url.format({pathname: Path.join(dir, 'index.m3u8')});
              rec.remoteUrl = itemUrl;

              this.recorders.push(rec);
            }

            groupItem.quotedString('uri', rec.localUrl);
          }
        });

        // start all recordings
        this.recorders.forEach((recording) => {
          recording.start();
        });

        this.index.iframes = [];
      }
      else {
        this.index.variants = [];
        this.index.groups = {};
        this.index.iframes = [];
      }
    }

    // hook end listener
    this.reader.on('end', () => {

      this.index.ended = true;
      this.flushIndex((/*err*/) => {

        debug('done');
      });
    });

    if (this.decrypt) {
      this.decrypt.base = this.reader.baseUrl;
    }
  }

  // validate update
  if (this.index.target_duration > update.target_duration) {
    throw new Error('Invalid index');
  }
};

HlsStreamRecorder.prototype.process = function (segmentInfo, next) {

  if (segmentInfo.type === 'segment') {
    return this.processSegment(segmentInfo, next);
  }

  if (segmentInfo.type === 'init') {
    return this.processInfo(segmentInfo, next);
  }

  debug('unknown segment type: ' + segmentInfo.type);

  return next();
};

HlsStreamRecorder.prototype.processInfo = function (segmentInfo, callback) {

  const meta = segmentInfo.file;
  const uri = `${this.segmentName(this.mapSeq, true)}.${Mime.extension(meta.mime)}`;

  this.writeStream(segmentInfo.stream, uri, (err, bytesWritten) => {

    // only to report errors
    if (err) debug('stream error', err.stack || err);

    const map = new M3U8Parse.AttrList();

    map.quotedString('uri', uri);

    // handle byterange
    if (this.collect) {
      map.quotedString('byterange', `${bytesWritten}@${this.uploader.segmentBytes - bytesWritten}`);
    }

    this.nextMap = map;
    return callback();
  });

  this.mapSeq++;
};

HlsStreamRecorder.prototype.processSegment = function (segmentInfo, callback) {

  let segment = new M3U8Parse.M3U8Segment(segmentInfo.segment.details, true);
  let meta = segmentInfo.file;

  // mark discontinuities
  if (this.nextSegmentSeq !== -1 &&
      this.nextSegmentSeq !== segmentInfo.segment.seq) {
    segment.discontinuity = true;
  }
  this.nextSegmentSeq = segmentInfo.segment.seq + 1;

  // create our own uri
  segment.uri = `${this.segmentName(this.seq)}.${Mime.extension(meta.mime)}`;

  // add map info
  if (this.nextMap) {
    segment.map = this.nextMap;
    this.nextMap = null;
  }

  delete segment.byterange;

  // save the stream segment
  SegmentDecrypt.decrypt(segmentInfo.stream, segmentInfo.segment.details.keys, this.decrypt, (err, stream, decrypted) => {
 
    if (err) {
      console.error('decrypt failed', err.stack);
      stream = segmentInfo.stream;
    }
    else if (decrypted) {
      segment.keys = null;
    }

    this.writeStream(stream, segment.uri, (err, bytesWritten) => {

      // only to report errors
      if (err) debug('stream error', err.stack || err);

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
      this.flushIndex(callback);
    });

    this.seq++;
  });
};

HlsStreamRecorder.prototype.writeStream = function (stream, name, callback) {

  this.uploader.pushSegment(stream, name).then((written) => callback(null, written), callback);
};

HlsStreamRecorder.prototype.variantName = function(info, index) {

  return `v${index}`;
};

HlsStreamRecorder.prototype.groupSrcName = function(info, index) {

  let lang = (info.quotedString('language') || '').replace(/\W/g, '').toLowerCase();
  let id = (info.quotedString('group-id') || 'unk').replace(/\W/g, '').toLowerCase();
  return `grp/${id}/${lang ? lang + '-' : ''}${index}`;
};

HlsStreamRecorder.prototype.segmentName = function(seqNo, isInit) {

  const name = (n) => {

    let next = ~~(n / 26);
    let chr = String.fromCharCode(97 + n % 26); // 'a' + n
    if (next) return name(next - 1) + chr;
    return chr;
  };

  return this.collect ? 'stream' : (isInit ? 'init-' : '') + name(seqNo);
};

HlsStreamRecorder.prototype.flushIndex = function(cb) {

  this.uploader.flushIndex(this.index).then(() => cb(), cb);
};

HlsStreamRecorder.prototype.recorderForUrl = function(remoteUrl) {

  let idx, len = this.recorders.length;
  for (idx = 0; idx < len; idx++) {
    let rec = this.recorders[idx];
    if (rec.remoteUrl === remoteUrl) {
      return rec;
    }
  }

  return null;
};


const hlsrecorder = module.exports = function hlsrecorder(reader, dst, options) {

  return new HlsStreamRecorder(reader, dst, options);
};

hlsrecorder.HlsStreamRecorder = HlsStreamRecorder;
