var path = require('path'),
    util = require('util'),
    carrier = require('carrier'),
    debug = require('debug')('hls:m3u8');

exports.M3U8Playlist = M3U8Playlist;
exports.M3U8Segment = M3U8Segment;
exports.ParserError = ParserError;

exports.parse = parse;
//exports.stringify = stringify;

function M3U8Playlist() {
  this.variant = false;

  // initialize to default values
  this.version = 1; // V1
  this.allow_cache = true;
  this.i_frames_only = false; // V4
  this.target_duration = undefined;
  this.first_seq_no = 0;
  this.type = undefined; // V?
  this.ended = false;

  this.segments = [];

  // for variant streams
  this.programs = {};
  this.groups = {};
}

M3U8Playlist.prototype.PlaylistType = {
  EVENT: 'EVENT',
  VOD: 'VOD'
};

M3U8Playlist.prototype.totalDuration = function() {
  return this.segments.reduce(function(sum, segment) {
    return sum + segment.duration;
  }, 0);
};

M3U8Playlist.prototype.isLive = function() {
  return !(this.ended || this.type === this.PlaylistType.VOD);
};

M3U8Playlist.prototype.startSeqNo = function(full) {
  if (!this.isLive() || full) return this.first_seq_no;

  var duration = this.target_duration * 3;
  for (var i=this.segments.length-1; i>0; i--) {
    duration -= this.segments[i].duration;
    if (duration < 0) break;
  }
  // TODO: validate that correct seqNo is returned
  return this.first_seq_no + i;
};

M3U8Playlist.prototype.lastSeqNo = function() {
  return this.first_seq_no + this.segments.length - 1;
};

M3U8Playlist.prototype.getSegment = function(seqNo) {
  // TODO: should we check for number type and throw if not?
  var index = seqNo-this.first_seq_no;
  if (index < 0 || index > this.segments.length)
    return null;
  return this.segments[index];
};

M3U8Playlist.prototype.toString = function() {
  // TODO:
  return 'M3U8Playlist';
};

function M3U8Segment(uri, meta, version) {
  this.duration = meta.duration;
  this.title = meta.title;
  this.uri = uri;
  this.discontinuity = meta.discontinuity || false;

  // optional
  if (meta.program_time)
    this.program_time = meta.program_time;
  if (meta.key)
    this.key = meta.key;

  if (version >= 5 && meta.map)
    this.map = meta.map;
}

M3U8Segment.prototype.toString = function() {
  // TODO: support writing all the information
  return '#EXTINF '+this.duration.toFixed(3)+','+this.title + '\n' + this.uri + '\n';
};

function parse(stream, cb) {
  var m3u8 = new M3U8Playlist(),
      line_no = 0,
      meta = {};

  var cr = carrier.carry(stream);
  cr.on('line', ParseLine);
  cr.on('end', Complete);

  function ReportError(err) {
    cr.removeListener('line', ParseLine);
    cr.removeListener('end', Complete);
    cb(err);
  }

  function Complete() {
    debug('result', m3u8);
    cb(null, m3u8);
  }

  function ParseExt(cmd, arg) {
    if (!(cmd in extParser))
      return false;

    debug('parsing ext', cmd, arg);
    extParser[cmd](arg);
    return true;
  }
  
  // AttrList's are currently handled without any implicit knowledge of key/type mapping
  function ParseAttrList(input) {
    // TODO: handle newline escapes in quoted-string's
    var re = /(.+?)=((?:\".*?\")|.*?)(?:,|$)/g;
//    var re = /(.+?)=(?:(?:\"(.*?)\")|(.*?))(?:,|$)/g;
    var match, attrs = {};
    while ((match = re.exec(input)) !== null)
      attrs[match[1].toLowerCase()] = match[2];

    debug('parsed attributes', attrs);
    return attrs;
  }

  function unquote(str) {
    return str.slice(1,-1);
  }

  function ParseLine(line) {
    line_no += 1;

    if (line_no === 1) {
      if (line !== '#EXTM3U')
        return ReportError(new ParserError('Missing required #EXTM3U header', line, line_no));
      return;
    }

    if (!line.length) return; // blank lines are ignored (3.1)
        
    if (line[0] === '#') {
      var matches = line.match(/^(#EXT[^:]*):?(.*)/);
      if (!matches)
        return debug('ignoring comment', line);

      var cmd = matches[1],
          arg = matches[2];

      if (!ParseExt(cmd, arg))
        return ReportError(new ParserError('Unknown #EXT: '+cmd, line, line_no));
    } else if (m3u8.variant) {
      var id = meta.info['program-id'];
      if (!(id in m3u8.programs))
        m3u8.programs[id] = [];

      meta.uri = line;
      m3u8.programs[id].push(meta);
      meta = {};
    } else {
      if (!('duration' in meta))
        return ReportError(new ParserError('Missing #EXTINF before media file URI', line, line_no));

      m3u8.segments.push(new M3U8Segment(line, meta, m3u8.version));
      meta = {};
    }
  }

  // TODO: add more validation logic
  var extParser = {
    '#EXT-X-VERSION': function(arg) {
      m3u8.version = parseInt(arg, 10);

      if (m3u8.version >= 4)
        for (var attrname in extParserV4) { extParser[attrname] = extParser[attrname]; }
      if (m3u8.version >= 5)
        for (var attrname in extParserV5) { extParser[attrname] = extParser[attrname]; }
    },
    '#EXT-X-TARGETDURATION': function(arg) {
      m3u8.target_duration = parseInt(arg, 10);
    },
    '#EXT-X-ALLOW-CACHE': function(arg) {
      m3u8.allow_cache = (arg!=='NO');
    },
    '#EXT-X-MEDIA-SEQUENCE': function(arg) {
      m3u8.first_seq_no = parseInt(arg, 10);
    },
    '#EXT-X-PLAYLIST-TYPE': function(arg) {
      m3u8.type = arg;
    },
    '#EXT-X-ENDLIST': function(arg) {
      m3u8.ended = true;
    },

    '#EXTINF': function(arg) {
      var n = arg.split(',');
      meta.duration = parseFloat(n.shift());
      meta.title = n.join(',');

      if (meta.duration <= 0)
        return ReportError(new ParserError('Invalid duration', line, line_no));
    },
    '#EXT-X-KEY': function(arg) {
      meta.key = ParseAttrList(arg);
    },
    '#EXT-X-PROGRAM-DATE-TIME': function(arg) {
      meta.program_time = new Date(arg);
    },
    '#EXT-X-DISCONTINUITY': function(arg) {
      meta.discontinuity = true;
    },

    // variant
    '#EXT-X-STREAM-INF': function(arg) {
      m3u8.variant = true;
      meta.info = ParseAttrList(arg);
    },
    // variant v4 since variant streams are not required to specify version
    '#EXT-X-MEDIA': function(arg) {
      //m3u8.variant = true;
      var attrs = ParseAttrList(arg),
          id = unquote(attrs['group-id']);

      if (!(id in m3u8.groups)) {
        m3u8.groups[id] = [];
        m3u8.groups[id].type = attrs.type;
      }
      m3u8.groups[id].push(attrs);
    },
    '#EXT-X-I-FRAME-STREAM-INF': function(arg) {
      m3u8.variant = true;
      debug('not yet supported', '#EXT-X-I-FRAME-STREAM-INF');
    }
  };

  var extParserV4 = {
    '#EXT-X-I-FRAMES-ONLY': function(arg) {
      m3u8.i_frames_only = true;
    },
    '#EXT-X-BYTERANGE': function(arg) {
      var n = arg.split('@');
      meta.byterange = {length:parseInt(n[0], 10)};
      if (n.length > 1)
        meta.byterange.offset = parseInt(n[1], 10);
    }
  }

  var extParserV5 = {
    '#EXT-X-MAP': function(arg) {
      meta.map = ParseAttrList(arg);
    }
  }
}

function ParserError(msg, line, line_no, constr) {
  Error.captureStackTrace(this, constr || this);
  this.message = msg || 'Error';
  this.line = line;
  this.line_no = line_no;
}
util.inherits(ParserError, Error);
ParserError.prototype.name = 'Parser Error';
