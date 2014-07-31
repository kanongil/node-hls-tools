"use strict";

// stream from hls source

var util = require('util'),
    url = require('url'),
    zlib = require('zlib'),
    assert = require('assert');

var extend = require('xtend'),
    oncemore = require('oncemore'),
    m3u8parse = require('m3u8parse'),
    uristream = require('uristream'),
    debug = require('debug')('hls:reader');

var Readable = require('readable-stream');

var noop = function noop() {};

function HlsSegmentObject(seq, segment, meta, stream) {
  this.seq = seq;
  this.segment = segment;
  this.meta = meta;
  this.stream = stream;
}

function fetchfrom(reader, seqNo, segment, cb) {
  var segmentUrl = url.resolve(reader.baseUrl, segment.uri);
  var probe = !!reader.noData;

  debug('fetching segment', segmentUrl);
  var stream = uristream(segmentUrl, { probe:probe, highWaterMark:100 * 1000 * 1000 });

  function finish(err, res) {
    stream.removeListener('meta', onmeta);
    stream.removeListener('end', onfail);
    stream.removeListener('error', onfail);
    cb(err, res);
  }

  function onmeta(meta) {
    debug('got segment meta', meta);

    if (reader.segmentMimeTypes.indexOf(meta.mime.toLowerCase()) === -1) {
      if (stream.abort) stream.abort();
      return stream.emit(new Error('Unsupported segment MIME type: ' + meta.mime));
    }

    finish(null, new HlsSegmentObject(seqNo, segment, meta, stream));
  }

  function onfail(err) {
    if (!err) err = new Error('No metadata');
    finish(err);
  }

  stream.on('meta', onmeta);
  stream.on('end', onfail);
  stream.on('error', onfail);

  return stream;
}

function checknext(reader) {
  var state = reader.readState;
  var index = reader.index;
  if (!state.active || state.fetching || state.nextSeq === -1 || !index)
    return null;

  var seq = state.nextSeq;
  var segment = index.getSegment(seq, true);

  if (segment) {
    // check if we need to stop
    if (reader.stopDate && segment.program_time > reader.stopDate)
      return reader.push(null);

    state.fetching = fetchfrom(reader, seq, segment, function(err, object) {
      state.fetching = null;
      if (err) reader.emit('error', err);

      if (seq === state.nextSeq)
        state.nextSeq++;

      if (object) {
        reader.watch[object.seq] = object.stream;
        oncemore(object.stream).once('end', 'error', function() {
          delete reader.watch[object.seq];
        });

        state.active = reader.push(object);
      }

      checknext(reader);
    });
  } else if (index.ended) {
    reader.push(null);
  } else if (!index.type && (index.lastSeqNo() < state.nextSeq - 1)) {
    // handle live stream restart
    state.nextSeq = index.startSeqNo(true);
    checknext(reader);
  }
}

function HlsStreamReader(src, options) {
  var self = this;

  options = options || {};
  if (typeof src === 'string')
    src = url.parse(src);

  this.url = src;
  this.baseUrl = src;

  this.fullStream = !!options.fullStream;
  this.noData = !!options.noData;

  // dates are inclusive
  this.startDate = options.startDate ? new Date(options.startDate) : null;
  this.stopDate = options.stopDate ? new Date(options.stopDate) : null;

  this.maxStallTime = options.maxStallTime || Infinity;

  this.extensions = options.extensions || {};

  this.index = null;
  this.readState = {
    nextSeq:-1,
    active:false,
    fetching:null
  };
  this.watch = {}; // used to stop buffering on expired segments

  this.indexStallSince = null;

  function getUpdateInterval(updated) {
    if (updated && self.index.segments.length) {
      self.indexStallSince = null;
      return Math.min(self.index.target_duration, self.index.segments[self.index.segments.length - 1].duration);
    } else {
      if (self.indexStallSince) {
        if ((Date.now() - +self.indexStallSince) > self.maxStallTime)
          return -1;
      } else {
        self.indexStallSince = new Date();
      }
      return self.index.target_duration / 2;
    }
  }

  function initialSeqNo() {
    var index = self.index;

    if (self.startDate)
      return index.seqNoForDate(self.startDate, true);
    else
      return index.startSeqNo(self.fullStream);
  }

  function updatecheck(updated) {
    if (updated) {
      if (self.readState.nextSeq === -1)
        self.readState.nextSeq = initialSeqNo();
      else if (self.readState.nextSeq < self.index.startSeqNo(true)) {
        debug('skipping ' + (self.index.startSeqNo(true) - self.readState.nextSeq) + ' invalidated segments');
        self.readState.nextSeq = self.index.startSeqNo(true);
      }

      // check watched segments
      for (var seq in self.watch) {
        if (!self.index.isValidSeqNo(seq)) {
          var stream = self.watch[seq];
          delete self.watch[seq];

          setTimeout(function () {
            debug('aborting discontinued segment download');
            if (!stream.ended && stream.abort) stream.abort();
          }, self.index.target_duration * 1000);
        }
      }

      self.emit('index', self.index);

      if (self.index.variant)
        return self.push(null);
    }
    checknext(self);

    if (self.index && !self.index.ended && self.readable) {
      var updateInterval = getUpdateInterval(updated);
      if (updateInterval <= 0)
        return self.emit('error', new Error('index stall'));
      debug('scheduling index refresh', updateInterval);
      setTimeout(updateindex, Math.max(1, updateInterval) * 1000);
    }
  }

  function updateindex() {
    if (!self.readable) return;
    var stream = uristream(url.format(self.url), { timeout:30 * 1000 });
    stream.on('meta', function(meta) {
      debug('got index meta', meta);

      if (self.indexMimeTypes.indexOf(meta.mime.toLowerCase()) === -1) {
        // FIXME: correctly handle .m3u us-ascii encoding
        if (stream.abort) stream.abort();
        return stream.emit('error', new Error('Invalid MIME type: ' + meta.mime));
      }

      self.baseUrl = meta.url;
    });

    m3u8parse(stream, { extensions:self.extensions }, function(err, index) {
      if (err) {
        self.emit('error', err);
        updatecheck(false);
      } else {
        var updated = true;
        if (self.index && self.index.lastSeqNo() === index.lastSeqNo()) {
          debug('index was not updated');
          updated = false;
        }

        self.index = index;
        updatecheck(updated);
      }
    });
  }

  Readable.call(this, extend(options, {objectMode:true, highWaterMark:options.highWaterMark || 0}));

  updateindex();
}
util.inherits(HlsStreamReader, Readable);

HlsStreamReader.prototype.indexMimeTypes = [
  'application/vnd.apple.mpegurl',
  'application/x-mpegurl',
  'audio/mpegurl',
];

HlsStreamReader.prototype.segmentMimeTypes = [
  'video/mp2t',
  'audio/aac',
  'audio/x-aac',
  'audio/ac3',
];

HlsStreamReader.prototype._read = function(/*n*/) {
  this.readState.active = true;
  checknext(this);
};


var hlsreader = module.exports = function hlsreader(url, options) {
  return new HlsStreamReader(url, options);
};

hlsreader.HlsSegmentObject = HlsSegmentObject;
hlsreader.HlsStreamReader = HlsStreamReader;
