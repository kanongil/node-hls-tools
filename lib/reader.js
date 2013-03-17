// stream from hls source

var util = require('util'),
    url = require('url'),
    zlib = require('zlib'),
    assert = require('assert');

var request = require('request'),
    oncemore = require('./oncemore'),
    debug = require('debug')('hls:reader');

try {
  var Readable = require('stream').Readable;
  assert(Readable);
  var Passthrough = null;
} catch (e) {
  var Readable = require('readable-stream');
  var Passthrough = require('readable-stream/passthrough');
}

var m3u8 = require('./m3u8');

function noop() {};

var DEFAULT_AGENT = util.format('hls-tools/v%s (http://github.com/kanongil/node-hls-tools) node.js/%s', require('../package').version, process.version);

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

function inheritErrors(stream) {
  stream.on('pipe', function(source) {
    source.on('error', stream.emit.bind(stream, 'error'));
  });
  stream.on('unpipe', function(source) {
    source.removeListener('error', stream.emit.bind(stream, 'error'));
  });
  return stream;
}

function getFileStream(srcUrl, options, cb) {
  assert(srcUrl.protocol);

  if (typeof options === 'function') {
    cb = options;
    options = {};
  }
  options = options || {};

  if (srcUrl.protocol === 'http:' || srcUrl.protocol === 'https:') {
    var headers = options.headers || {};
    if (!headers['user-agent']) headers['user-agent'] = DEFAULT_AGENT;
    if (!headers['accept-encoding']) headers['accept-encoding'] = ['gzip','deflate'];
    var start = options.start || 0;
    if (start > 0)
      headers['range'] = 'bytes=' + options.start + '-';

    var req = (options.probe ? request.head : request.get)({uri:url.format(srcUrl), pool:false, headers:headers, timeout:60*1000});
    req.on('error', onreqerror);
    req.on('response', onresponse);

    function onreqerror(err) {
      req.removeListener('error', onreqerror);
      req.removeListener('response', onresponse);
      cb(err);
    }

    function onresponse(res) {
      req.removeListener('error', onreqerror);
      req.removeListener('response', onresponse);

      if (res.statusCode !== 200 && res.statusCode !== 206) {
        req.abort();
        if (res.statusCode >= 500 && res.statusCode !== 501)
          return cb(new TempError('HTTP Server returned: '+res.statusCode));
        else
          return cb(new Error('Bad server response code: '+res.statusCode));
      }

      var size = res.headers['content-length'] ? parseInt(res.headers['content-length'], 10) : -1;

      // turn bad content-length into actual errors
      if (size >= 0 && !options.probe) {
        var accum = 0;
        res.on('data', function(chunk) {
          accum += chunk.length;
          if (accum > size)
            req.abort();
        });
        
        oncemore(res).once('end', 'error', function(err) {
          // TODO: make this a custom error?
          if (!err && accum !== size)
            stream.emit('error', new PartialError('Stream length did not match header', accum, size));
        });
      }

      // transparently handle gzip responses
      var stream = res;
      if (res.headers['content-encoding'] === 'gzip' || res.headers['content-encoding'] === 'deflate') {
        unzip = zlib.createUnzip();
        stream = stream.pipe(inheritErrors(unzip));
        size = -1;
      }

      // adapt old style streams for pre-streams2 node versions
      if (Passthrough && !(stream instanceof Readable))
        stream = stream.pipe(inheritErrors(new Passthrough()));

      // allow aborting the request
      stream.abort = req.abort.bind(req);

      // forward all future errors to response stream
      req.on('error', function(err) {
        if (stream.listeners('error').length !== 0)
          stream.emit('error', err);
      });

      // attach empty 'error' listener to keep it from ever throwing
      stream.on('error', noop);

      // extract meta information from header
      var typeparts = /^(.+?\/.+?)(?:;\w*.*)?$/.exec(res.headers['content-type']) || [null, 'application/octet-stream'],
          mimetype = typeparts[1].toLowerCase(),
          modified = res.headers['last-modified'] ? new Date(res.headers['last-modified']) : null;

      cb(null, stream, {url:url.format(req.uri), mime:mimetype, size:start+size, modified:modified});
    }
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
    getFileStream(self.url, function(err, stream, meta) {
      if (err) {
        if (self.index && self.keepConnection) {
          console.error('Failed to update index at '+url.format(self.url)+':', err.stack || err);
          return updatecheck(false);
        }
        return self.emit('error', err);
      }

      if (meta.mime !== 'application/vnd.apple.mpegurl' &&
          meta.mime !== 'application/x-mpegurl' && meta.mime !== 'audio/mpegurl')
        return self.emit('error', new Error('Invalid MIME type: '+meta.mime));
      // FIXME: correctly handle .m3u us-ascii encoding

      self.baseUrl = meta.url;
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
            if (err instanceof PartialError && err.processed > 0 &&
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

    debug('fetching segment', segmentUrl);
    getFileStream(url.parse(segmentUrl), {probe:!!self.noData, start:start}, function(err, stream, meta) {
      if (err) return cb(err);

      debug('got segment info', meta);
      if (meta.mime !== 'video/mp2t'/* && 
          meta.mime !== 'audio/aac' && meta.mime !== 'audio/x-aac' &&
          meta.mime !== 'audio/ac3'*/) {
        if (stream && stream.abort)
          stream.abort();
        return cb(new Error('Unsupported segment MIME type: '+meta.mime));
      }

      if (!start)
        self.emit('segment', seqNo, segment.duration, meta);

      if (stream) {
        debug('preparing to push stream to reader', meta.url);

        stream.on('error', onerror);

        function onerror(err) {
          debug('stream error', err);
          stream.removeListener('error', onerror);
          cb(err)
        }

        self.readState.stream = stream;
        self.readState.stream_started = false;
        self.readState.doneCb = function() {
          debug('finished with input stream', meta.url);
          stream.removeListener('error', onerror);
          cb(null);
        };

        // force a new _read in the future()
        if (self.push(''))
          self.stream_start();
      } else {
        process.nextTick(cb);
      }
    });
  }

  // allow piping content to self
  this.write = function(chunk) {
    self.push(chunk);
    return true;
  };
  this.end = function() {};

  this.stream_start = function() {
    var stream = self.readState.stream;
    if (stream && !self.readState.stream_started) {
      debug('pushing input stream to reader');

      stream.pipe(self);

      oncemore(stream).once('end', 'error', function(err) {
        clearTimeout(self.readState.timer);

        stream.unpipe(self);
        self.readState.stream = null;

        if (!err)
          self.readState.doneCb();
      });

      clearTimeout(self.readState.timer);

      // abort() indicates a temporal stream. Ie. ensure it is completed in a timely fashion
      if (self.index.isLive() && typeof stream.abort == 'function') {
        var duration = self.readState.currentSegment.duration || self.index.target_duration || 10;
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
      self.readState.stream_started = true;
    }
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

function TempError(msg) {
  Error.captureStackTrace(this, this);
  this.message = msg || 'TempError';
}
util.inherits(TempError, Error);
TempError.prototype.name = 'Temporary Error';

function PartialError(msg, processed, expected) {
  Error.captureStackTrace(this, this);
  this.message = msg || 'TempError';
  this.processed = processed || -1;
  this.expected = expected;
}
util.inherits(PartialError, Error);
PartialError.prototype.name = 'Partial Error';
