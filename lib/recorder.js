/*jslint node: true */

"use strict";

var fs = require('fs'),
    path = require('path'),
    url = require('url'),
    util = require('util');

var mime = require('mime-types'),
    streamprocess = require('streamprocess'),
    oncemore = require('oncemore'),
    m3u8parse = require('m3u8parse'),
    mkdirp = require('mkdirp'),
    writeFileAtomic = require('write-file-atomic'),
    debug = require('debug')('hls:recorder');

// add custom extensions
mime.extensions['audio/aac'] = ['aac'];
mime.extensions['audio/ac3'] = ['ac3'];

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

  this.recorders = [];
}

HlsStreamRecorder.prototype.start = function() {
  // TODO: make async?
  if (!fs.existsSync(this.dst))
    mkdirp.sync(this.dst);

  streamprocess(this.reader, this.process.bind(this));

  this.updateIndex(this.reader.index);
  this.reader.on('index', this.updateIndex.bind(this));
};

HlsStreamRecorder.prototype.updateIndex = function(update) {
  var self = this;

  if (!update) return;

  if (!this.index) {
    this.index = new m3u8parse.M3U8Playlist(update);
    if (!this.index.variant) {
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
        var offset = this.startOffset;
        if (!update.ended) {
          if (offset < 0) offset = Math.min(offset, -3 * this.index.target_duration);
        }
        this.index.start.decimalInteger('time-offset', offset);
      }
    } else {
      debug('programs', this.index.programs);
      if (this.subreader) {
        var programNo = Object.keys(this.index.programs)[0];
        var programs = this.index.programs[programNo];

        // remove backup sources
        var used = {};
        programs = programs.filter(function(program) {
          var bw = parseInt(program.info.bandwidth, 10);
          var res = !(bw in used);
          used[bw] = true;
          return res;
        });

        this.index.programs[programNo] = programs;

        programs.forEach(function(program, index) {
          var programUrl = url.resolve(self.reader.baseUrl, program.uri);
          debug('url', programUrl);

          // check for duplicate source urls
          var rec = this.recorderForUrl(programUrl);
          if (!rec || !rec.localUrl) {
            var dir = self.variantName(program.info, index);
            rec = new HlsStreamRecorder(self.subreader(programUrl), path.join(self.dst, dir), { startOffset: self.startOffset, collect: self.collect });
            rec.localUrl = url.format({pathname: path.join(dir, 'index.m3u8')});
            rec.remoteUrl = programUrl;

            this.recorders.push(rec);
          }

          program.uri = rec.localUrl;
        }, this);

        var allGroups = [];
        for (var group in this.index.groups)
          [].push.apply(allGroups, this.index.groups[group]);


        allGroups.forEach(function(groupItem, index) {
          var srcUri = groupItem.quotedString('uri');
          if (srcUri) {
            var itemUrl = url.resolve(self.reader.baseUrl, srcUri);
            debug('url', itemUrl);

            var rec = this.recorderForUrl(itemUrl);
            if (!rec || !rec.localUrl) {
              var dir = self.groupSrcName(groupItem, index);
              rec = new HlsStreamRecorder(self.subreader(itemUrl), path.join(self.dst, dir), { startOffset: self.startOffset, collect: self.collect });
              rec.localUrl = url.format({pathname: path.join(dir, 'index.m3u8')});
              rec.remoteUrl = itemUrl;

              this.recorders.push(rec);
            }

            groupItem.quotedString('uri', rec.localUrl);
          }
        }, this);

        // start all recordings
        this.recorders.forEach(function(recording) {
          recording.start();
        });

        this.index.iframes = {};
      } else {
        this.index.programs = {};
        this.index.groups = {};
        this.index.iframes = {};
      }
    }

    // hook end listener
    this.reader.on('end', function() {
      self.index.ended = true;
      self.flushIndex(function(/*err*/) {
        debug('done');
      });
    });
  }

  // validate update
  if (this.index.target_duration > update.target_duration)
    throw new Error('Invalid index');
};

HlsStreamRecorder.prototype.process = function(segmentInfo, next) {
  var self = this;

  var segment = new m3u8parse.M3U8Segment(segmentInfo.details, true);
  var meta = segmentInfo.file;

  // mark discontinuities
  if (this.nextSegmentSeq !== -1 &&
      this.nextSegmentSeq !== segmentInfo.seq)
    segment.discontinuity = true;
  this.nextSegmentSeq = segmentInfo.seq + 1;

  // create our own uri
  segment.uri = util.format('%s.%s', this.segmentName(this.seq), mime.extension(meta.mime));

  // handle byterange
  var first = self.index.segments.length === 0;
  var newFile = first || self.index.segments[self.index.segments.length - 1].uri !== segment.uri;
  if (this.collect) {
    segment.byterange = {
      length: 0,
      offset: newFile ? 0 : null
    }
  } else {
    delete segment.byterange;
  }

  // save the stream segment
  var stream = oncemore(segmentInfo.stream);
  stream.pipe(fs.createWriteStream(path.join(this.dst, segment.uri), { flags: newFile ? 'w' : 'a' }));

  var bytesWritten = 0;
  if (this.collect) {
    stream.on('data', function(chunk) {
      bytesWritten += chunk.length;
    });
  }

  stream.once('end', 'error', function(err) {
    // only to report errors
    if (err) debug('stream error', err.stack || err);

    if (segment.byterange)
      segment.byterange.length = bytesWritten;

    // update index
    self.index.segments.push(segment);
    self.flushIndex(next);
  });

  this.seq++;
};

HlsStreamRecorder.prototype.variantName = function(info, index) {
  return util.format('v%d', index);
};

HlsStreamRecorder.prototype.groupSrcName = function(info, index) {
  var lang = (info.quotedString('language') || '').replace(/\W/g, '').toLowerCase();
  var id = (info.quotedString('group-id') || 'unk').replace(/\W/g, '').toLowerCase();
  return util.format('grp/%s/%s%d', id, lang ? lang + '-' : '', index);
};

HlsStreamRecorder.prototype.segmentName = function(seqNo) {
  function name(n) {
    var next = ~~(n / 26);
    var chr = String.fromCharCode(97 + n % 26); // 'a' + n
    if (next) return name(next - 1) + chr;
    return chr;
  }
  return this.collect ? 'stream' : name(seqNo);
};

HlsStreamRecorder.prototype.flushIndex = function(cb) {
  var appendString, indexString = this.index.toString().trim();
  if (this.lastIndexString && indexString.lastIndexOf(this.lastIndexString, 0) === 0) {
    var lastLength = this.lastIndexString.length;
    appendString = indexString.substr(lastLength);
  }
  this.lastIndexString = indexString;

  if (appendString) {
    fs.appendFile(path.join(this.dst, 'index.m3u8'), appendString, cb);
  } else {
    writeFileAtomic(path.join(this.dst, 'index.m3u8'), indexString, cb);
  }
};

HlsStreamRecorder.prototype.recorderForUrl = function(remoteUrl) {
  var idx, len = this.recorders.length;
  for (idx = 0; idx < len; idx++) {
    var rec = this.recorders[idx];
    if (rec.remoteUrl === remoteUrl)
      return rec;
  }
  return null;
};


var hlsrecorder = module.exports = function hlsrecorder(reader, dst, options) {
  return new HlsStreamRecorder(reader, dst, options);
};

hlsrecorder.HlsStreamRecorder = HlsStreamRecorder;
