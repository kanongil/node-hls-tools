"use strict";

var fs = require('fs'),
    path = require('path'),
    url = require('url'),
    util = require('util');

var mime = require('mime'),
    streamprocess = require('streamprocess'),
    oncemore = require('oncemore'),
    m3u8parse = require('m3u8parse'),
    debug = require('debug')('hls:recorder');

function HlsStreamRecorder(reader, dst, options) {
  options = options || {};

  this.reader = reader;
  this.dst = dst; // target directory 

  this.nextSegmentSeq = -1;
  this.seq = 0;
  this.index = null;

  this.startOffset = parseFloat(options.startOffset);
  this.subreader = options.subreader;
}

HlsStreamRecorder.prototype.start = function() {
  // TODO: make async?
  if (!fs.existsSync(this.dst))
    fs.mkdirSync(this.dst);

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
      this.index.segments = [];
      this.index.first_seq_no = self.seq;
      this.index.type = 'EVENT';
      this.index.ended = false;
      this.index.discontinuity_sequence = 0; // not allowed in event playlists
      if (!isNaN(this.startOffset)) {
        var offset = this.startOffset;
        if (offset < 0) offset = Math.min(offset, -3 * this.target_duration);
        this.index.start = { offset: offset };
      }
    } else {
      debug('programs', this.index.programs);
      if (this.subreader) {
        for (var programNo in this.index.programs) {
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
            var dir = self.variantName(program.info, index);
            program.uri = path.join(dir, 'index.m3u8');
            program.recorder = new HlsStreamRecorder(self.subreader(programUrl), path.join(self.dst, dir), { startOffset: self.startOffset }).start();
          });
        }

        // TODO: handle groups!!
        this.index.groups = {};
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

HlsStreamRecorder.prototype.process = function(obj, next) {
  var self = this;

  var segment = new m3u8parse.M3U8Segment(obj.segment);
  var meta = obj.meta;

  // mark discontinuities
  if (this.nextSegmentSeq !== -1 &&
      this.nextSegmentSeq !== obj.seq)
    segment.discontinuity = true;
  this.nextSegmentSeq = obj.seq + 1;

  // create our own uri
  segment.uri = util.format('%s.%s', this.segmentName(this.seq), mime.extension(meta.mime));

  // manually set iv if sequence based, since we generate our own sequence numbering 
  if (segment.key && !segment.key.iv) {
    var seqStr = obj.seq.toString();
    segment.key.iv = '0x00000000000000000000000000000000'.slice(-seqStr.length) + seqStr;
    if (this.index.version > 2) {
      this.index.version = 2;
      debug('changed index version to:', this.index.version);
    }
  }

  // save the stream segment
  var stream = oncemore(obj.stream);
  stream.pipe(fs.createWriteStream(path.join(this.dst, segment.uri)));
  stream.once('end', 'error', function(err) {
    // only to report errors
    if (err) debug('stream error', err.stack || err);

    // update index
    self.index.segments.push(segment);
    self.flushIndex(next);
  });

  this.seq++;
};

HlsStreamRecorder.prototype.variantName = function(info, index) {
  return util.format('v%d', index);
};

HlsStreamRecorder.prototype.segmentName = function(seqNo) {
  function name(n) {
    var next = ~~(n / 26);
    var chr = String.fromCharCode(97 + n % 26); // 'a' + n
    if (next) return name(next - 1) + chr;
    return chr;
  }
  return name(seqNo);
};

HlsStreamRecorder.prototype.flushIndex = function(cb) {
  // TODO: make atomic by writing to temp file & renaming
  fs.writeFile(path.join(this.dst, 'index.m3u8'), this.index, cb);
};


var hlsrecorder = module.exports = function hlsrecorder(reader, dst, options) {
  return new HlsStreamRecorder(reader, dst, options);
};

hlsrecorder.HlsStreamRecorder = HlsStreamRecorder;
