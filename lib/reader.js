// stream from hls source

var util = require('util'),
    url = require('url'),
    assert = require('assert');

var async = require('async'),
    http = require('http-get'),
    debug = require('debug')('hls:reader');

try {
  var Readable = require('stream').Readable;
  assert(Readable);
} catch (e) {
  var Readable = require('readable-stream');
}

var m3u8 = require('./m3u8');

var DEFAULT_AGENT = util.format('hls-tools/v%s (http://github.com/kanongil/node-hls-tools) node.js/%s', '0.0.0', process.version);

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

// ensure function is never run more than once
function once(fn) {
  var called = false;
  return function() {   
    var call = !called;
    called = true;
    if(call) fn.apply(this, arguments);
  };
}

function getFileStream(srcUrl, options, cb) {
  assert(srcUrl.protocol);

  if (typeof options === 'function') {
    cb = options;
    options = {};
  }

  if (srcUrl.protocol === 'http:' || srcUrl.protocol === 'https:') {
    var headers = options.headers || {};
    if (!headers['user-agent']) headers['user-agent'] = DEFAULT_AGENT;

    // http-get will occasionally call the callback multiple times... :–(
    (options.probe ? http.head : http.get)({url:url.format(srcUrl), stream:true, headers:headers}, once(function(err, res) {
    	if (err) return cb(err);

      var statusCode = res.code || res.stream.statusCode;
      if (statusCode !== 200) {
        if (res.stream)
          res.stream.destroy();
        return cb(new Error('Bad server response code: '+statusCode));
      }

      var typeparts = /^(.+?\/.+?)(?:;\w*.*)?$/.exec(res.headers['content-type']) || [null, 'application/octet-stream'],
          mimetype = typeparts[1].toLowerCase(),
          size = res.headers['content-length'] ? parseInt(res.headers['content-length'], 10) : -1,
          modified = res.headers['last-modified'] ? new Date(res.headers['last-modified']) : null;

      res.stream.resume(); // for some reason http-get pauses the stream for the callback
      cb(null, res.stream, {url:res.url || url.format(srcUrl), mime:mimetype, size:size, modified:modified});
    }));
  } else {
    process.nextTick(function() {
      cb(new Error('Unsupported protocol: '+srcUrl.protocol));
    });
  }

/*    if (srcUrl.protocol === 'file:') {
      
  } else if (srcUrl.protocol === 'data:') {
    //var regex = /^data:(.+\/.+);base64,(.*)$/;
    // add content-type && content-length headers
  } else {
      
  }*/
}

function HlsStreamReader(src, options) {
  var self = this;

  options = options || {};
  if (typeof src === 'string')
    src = url.parse(src);

  this.url = src;
  this.baseUrl = src;

  this.prebufferSize = options.prebufferSize || 0;
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

  if (this.prebufferSize) {
    var lwm = options.lowWaterMark || 0;
    var hwm = options.highWaterMark || this.prebufferSize * 2;
    options.lowWaterMark = Math.max(this.prebufferSize, lwm);
    options.highWaterMark = Math.max(hwm, lwm);
    this.once('readable', function() {
      self._readableState.lowWaterMark = ~~lwm;
    });
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
      var updateInterval = updated ? self.index.segments[self.index.segments.length-1].duration : self.index.target_duration / 2;
      debug('scheduling index refresh', updateInterval);
      setTimeout(updateindex, Math.max(1, updateInterval)*1000);
    }
  }

  function updateindex() {
    getFileStream(self.url, function(err, stream, meta) {
      if (err) {
        if (self.index && self.keepConnection) {
          console.error('Failed to update index at '+url.format(self.url)+':', err.stack || err);
          return updatecheck();
        }
        return self.emit('error', err);
      }

      if (meta.mime !== 'application/vnd.apple.mpegurl' &&
          meta.mime !== 'application/x-mpegurl' && meta.mime !== 'audio/mpegurl')
        return self.emit('error', new Error('Invalid MIME type: '+meta.mime));
      // FIXME: correctly handle .m3u us-ascii encoding

      self.baseUrl = meta.url;
      m3u8.parse(stream, function(err, index) {
        if (err) return self.emit('error', err);

        var updated = true;
        if (self.index && self.index.lastSeqNo() === index.lastSeqNo()) {
          debug('index was not updated');
          updated = false;
        }

        self.index = index;

        updatecheck(updated);
      });
    });
  }
  updateindex();

  function checkcurrent() {
    if (self.readState.currentSegment) return; // already processing

    self.readState.currentSegment = self.index.getSegment(self.readState.currentSeq);
    if (self.readState.currentSegment) {
      var url = self.readState.currentSegment.uri;
      fetchfrom(self.readState.currentSeq, self.readState.currentSegment, function(err, transferred) {
        self.readState.currentSegment = null;
        if (err) {
          if (!self.keepConnection) return self.emit('error', err);
          console.error('While fetching '+url+':', err.stack || err);
          if (!transferred) return; // TODO: retry with a range header
        }
        self.readState.currentSeq++;
        checkcurrent();
      });
    } else if (self.index.ended)
      self.end();
    else if (!self.index.type && (self.index.lastSeqNo() < self.readState.currentSeq-1)) {
      // handle live stream restart
      self.readState.currentSeq = self.index.startSeqNo(true);
      checkcurrent();
    }
  }

  function fetchfrom(seqNo, segment, cb) {
    var segmentUrl = url.resolve(self.baseUrl, segment.uri)

    debug('fetching segment', segmentUrl);
    getFileStream(url.parse(segmentUrl), {probe:!!self.noData}, function(err, stream, meta) {
      if (err) return cb(err);

      debug('got segment info', meta);
      if (meta.mime !== 'video/mp2t'/* && 
          meta.mime !== 'audio/aac' && meta.mime !== 'audio/x-aac' &&
          meta.mime !== 'audio/ac3'*/)
        return cb(new Error('Unsupported segment MIME type: '+meta.mime));

      self.emit('segment', seqNo, segment.duration, meta);

      if (stream) {
        debug('pushing input stream to reader');

        var totalBytes = 0;
        stream.on('data', function(chunk) {
          totalBytes += chunk.length;
          self.push(chunk); // intentionally ignore the result to buffer input as fast as possible
        });
        stream.on('error', Done);
        stream.on('end', Done);
        stream.on('close', Done);

        self.readState.stream = stream;
        self.stream_start(true, !self.push(new Buffer(0)));

        function Done(err) {
          debug('finished with input stream');

          stream.removeListener('error', Done);
          stream.removeListener('end', Done);
          stream.removeListener('close', Done);

          self.readState.stream = null;

          // FIXME: is this required? or already handled by http-get?
          if (!err && (totalBytes !== meta.size))
            err = new Error('Invalid returned stream length');

          cb(err, totalBytes);
        }
      } else {
        process.nextTick(cb);
      }
    });
  }

  this.stream_start = function(fresh, blocked) {
    if (fresh) {
      self.readState.stream_started = false;
      if (self.readState.timer) {
        clearTimeout(self.readState.timer);
        self.readState.timer = null;
      }

      if (blocked) return self.readState.stream.pause();
    }

    if (self.readState.stream_started) return;

    var stream = self.readState.stream;
    if (!stream) return;

    if (typeof stream.destroy == 'function') {
      debug('timed out waiting for data');
      var duration = self.readState.currentSegment.duration || self.index.target_duration || 10;
      self.readState.timer = setTimeout(function() {
        if (self.readState.stream)
          self.readState.stream.destroy();
        self.readState.timer = null;
      }, 1.5*duration*1000);
    }
    self.readState.stream_started = true;
    stream.resume();
  }

  Readable.call(this, options);
}
util.inherits(HlsStreamReader, Readable);

HlsStreamReader.prototype._read = function(n, cb) {
  this.stream_start();
};

function hlsreader(url, options) {
  return new HlsStreamReader(url, options);
}
