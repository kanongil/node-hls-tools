// stream from hls source

var util = require('util'),
    url = require('url'),
    zlib = require('zlib'),
    assert = require('assert');

var request = require('request'),
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
hlsreader.HlsStreamReader = HlsStreamReader;

/*
options:
  startSeq*
  noData // don't emit any data - useful for analyzing the stream structure

  maxRedirects*
  cacheDir*
  headers* // allows for custom user-agent, cookies, auth, etc
  
emits:
  index (m3u8)
  segment (seqNo, duration, datetime, size?, )
*/

function HlsStreamReader(src, options) {
  var self = this;

  options = options || {};
  if (typeof src === 'string')
    src = url.parse(src);

  this.url = src;
  this.baseUrl = src;

  this.fullStream = !!options.fullStream;
  this.keepConnection = !!options.keepConnection;
  this.noData = !!options.noData;

  this.indexStream = null;
  this.index = null;
  this.readState = {
    currentSeq:-1,
    currentSegment:null,
    stream:null
  }

  function getUpdateInterval(updated) {
    if (updated && self.index.segments.length)
      return Math.min(self.index.target_duration, self.index.segments[self.index.segments.length-1].duration);
    else
      return self.index.target_duration / 2;
  }

  function updatecheck(updated) {
    if (updated) {
      if (self.readState.currentSeq===-1)
        self.readState.currentSeq = self.index.startSeqNo(self.fullStream);
      else if (self.readState.currentSeq < self.index.startSeqNo(true))
        self.readState.currentSeq = self.index.startSeqNo(true);

      self.emit('index', self.index);

      if (self.index.variant)
        return self.end();
    }
    checkcurrent();

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

  function checkcurrent() {
    if (self.readState.currentSegment) return; // already processing

    self.readState.currentSegment = self.index.getSegment(self.readState.currentSeq);
    if (self.readState.currentSegment) {
      var url = self.readState.currentSegment.uri;

      function tryfetch(start) {
        var seq = self.readState.currentSeq;
        fetchfrom(seq, self.readState.currentSegment, start, function(err) {
          if (err) {
            if (!self.keepConnection) return self.emit('error', err);
            console.error('While fetching '+url+':', err.stack || err);

            // retry with missing range if it is still relevant
            if (err instanceof uristream.PartialError && err.processed > 0 &&
                self.index.getSegment(seq))
                return tryfetch(start + err.processed);
          }

          self.readState.currentSegment = null;
          if (seq === self.readState.currentSeq)
            self.readState.currentSeq++;

          checkcurrent();
        });
      }
      tryfetch(0);
    } else if (self.index.ended)
      self.end();
    else if (!self.index.type && (self.index.lastSeqNo() < self.readState.currentSeq-1)) {
      // handle live stream restart
      self.readState.currentSeq = self.index.startSeqNo(true);
      checkcurrent();
    }
  }

  function fetchfrom(seqNo, segment, start, cb) {
    var segmentUrl = url.resolve(self.baseUrl, segment.uri)
    var probe = !!self.noData;

    debug('fetching segment', segmentUrl);
    var stream = uristream(segmentUrl, { probe:probe, start:start, highWaterMark:100*1000*1000 });
    stream.on('meta', function(meta) {
      debug('got segment info', meta);
      if (meta.mime !== 'video/mp2t'/* && 
          meta.mime !== 'audio/aac' && meta.mime !== 'audio/x-aac' &&
          meta.mime !== 'audio/ac3'*/) {
        if (stream.abort) stream.abort();
        return stream.emit(new Error('Unsupported segment MIME type: '+meta.mime));
      }

      // abort() indicates a temporal stream. Ie. ensure it is completed in a timely fashion
      if (self.index.isLive() && typeof stream.abort == 'function') {
        var duration = segment.duration || self.index.target_duration || 10;
        duration = Math.min(duration, self.index.target_duration || 10);
        self.readState.timer = setTimeout(function() {
          if (self.readState.stream) {
            debug('timed out waiting for data');
            self.readState.stream.abort();
          }
          // TODO: ensure Done() is always called
          self.readState.timer = null;
        }, 1.5*duration*1000);
      }

      if (start === 0)
        self.emit('segment', seqNo, segment.duration, meta);
    });

    stream.pipe(self);
    oncemore(stream).once('end', 'error', function(err) {
      clearTimeout(self.readState.timer);

      stream.unpipe(self);
      self.readState.stream = null;

      if (err) debug('stream error', err);
      else debug('finished with input stream', stream.meta.url);

      cb(err);
    });

    self.readState.stream = stream;
  }

  // allow piping content to self
  this.write = this.push.bind(this);
  this.end = function() {};

  Readable.call(this, options);
}
util.inherits(HlsStreamReader, Readable);

HlsStreamReader.prototype._read = function(n, cb) {
  this.emit('drain');
};

function hlsreader(url, options) {
  return new HlsStreamReader(url, options);
}
