// stream from hls source

var util = require('util'),
    url = require('url'),
    zlib = require('zlib'),
    assert = require('assert');

var request = require('request'),
    extend = require('xtend'),
    oncemore = require('./oncemore'),
    uristream = require('./uristream'),
    debug = require('debug')('hls:reader');

try {
  var Readable = require('stream').Readable;
  assert(Readable);
} catch (e) {
  var Readable = require('readable-stream');
}

var m3u8 = require('./m3u8');

function noop() {};

module.exports = hlsreader;
hlsreader.HlsSegmentObject = HlsSegmentObject;
hlsreader.HlsStreamReader = HlsStreamReader;

function HlsSegmentObject(seq, segment, meta, stream) {
  this.seq = seq;
  this.segment = segment;
  this.meta = meta;
  this.stream = stream;
}

function checknext(reader) {
  var state = reader.readState;
  var index = reader.index;
  if (!state.active || state.fetching || !index)
    return;

  var seq = state.nextSeq;
  var segment = index.getSegment(seq);

  if (segment) {
    state.fetching = fetchfrom(reader, seq, segment, function(err, object) {
      state.fetching = null;
      if (err) reader.emit('error', err);

      if (seq === state.nextSeq)
        state.nextSeq++;

      state.active = reader.push(object);
      checknext(reader);
    });
  } else if (index.ended) {
    reader.push(null);
  } else if (!index.type && (index.lastSeqNo() < state.nextSeq-1)) {
    // handle live stream restart
    state.nextSeq = index.startSeqNo(true);
    checknext(reader);
  }
}

function fetchfrom(reader, seqNo, segment, cb) {
  var segmentUrl = url.resolve(reader.baseUrl, segment.uri)
  var probe = !!reader.noData;

  debug('fetching segment', segmentUrl);
  var stream = uristream(segmentUrl, { probe:probe, highWaterMark:100*1000*1000 });
  stream.on('meta', onmeta);
  stream.on('end', onfail);
  stream.on('error', onfail);

  function cleanup() {
    stream.removeListener('meta', onmeta);
    stream.removeListener('end', onfail);
    stream.removeListener('error', onfail);
  }

  function onmeta(meta) {
    debug('got segment info', meta);

    if (meta.mime !== 'video/mp2t'/* && 
        meta.mime !== 'audio/aac' && meta.mime !== 'audio/x-aac' &&
        meta.mime !== 'audio/ac3'*/) {
      if (stream.abort) stream.abort();
      return stream.emit(new Error('Unsupported segment MIME type: '+meta.mime));
    }

    cleanup();
    cb(null, new HlsSegmentObject(seqNo, segment, meta, stream));
  }

  function onfail(err) {
    if (!err) err = new Error('No metadata');

    cleanup();
    cb(err)
  }

  return stream;
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

  this.index = null;
  this.readState = {
    nextSeq:-1,
    currentSegment:null,
  }

  function getUpdateInterval(updated) {
    if (updated && self.index.segments.length)
      return Math.min(self.index.target_duration, self.index.segments[self.index.segments.length-1].duration);
    else
      return self.index.target_duration / 2;
  }

  function updatecheck(updated) {
    if (updated) {
      if (self.readState.nextSeq===-1)
        self.readState.nextSeq = self.index.startSeqNo(self.fullStream);
      else if (self.readState.nextSeq < self.index.startSeqNo(true)) {
        debug('skipping '+(self.index.startSeqNo(true)-self.readState.nextSeq)+' invalidated segments');
        self.readState.nextSeq = self.index.startSeqNo(true);
      }

      self.emit('index', self.index);

      if (self.index.variant)
        return self.end();
    }
    checknext(self);

    if (!self.index.ended) {
      var updateInterval = getUpdateInterval(updated);
      debug('scheduling index refresh', updateInterval);
      setTimeout(updateindex, Math.max(1, updateInterval)*1000);
    }
  }

  function updateindex() {
    var stream = uristream(url.format(self.url), { timeout:30*1000 });
    stream.on('meta', function(meta) {
      if (meta.mime !== 'application/vnd.apple.mpegurl' &&
          meta.mime !== 'application/x-mpegurl' && meta.mime !== 'audio/mpegurl')
        return stream.emit('error', new Error('Invalid MIME type: '+meta.mime));
      // FIXME: correctly handle .m3u us-ascii encoding

      self.baseUrl = meta.url;
    });

    m3u8.parse(stream, function(err, index) {
      if (err) {
        if (self.index && self.keepConnection) {
          console.error('Failed to parse index at '+url.format(self.url)+':', err.stack || err);
          return updatecheck(false);
        }
        return self.emit('error', err);
      }

      var updated = true;
      if (self.index && self.index.lastSeqNo() === index.lastSeqNo()) {
        debug('index was not updated');
        updated = false;
      }

      self.index = index;
      updatecheck(updated);
    });
  }
  updateindex();

  Readable.call(this, extend(options, {objectMode:true}));
}
util.inherits(HlsStreamReader, Readable);

HlsStreamReader.prototype._read = function(n) {
  this.readState.active = true;
  checknext(this);
};

function hlsreader(url, options) {
  return new HlsStreamReader(url, options);
}
