// stream from hls source

var util = require('util'),
    url = require('url'),
    zlib = require('zlib'),
    assert = require('assert');

var request = require('request'),
    debug = require('debug')('hls:reader');

try {
  var Readable = require('stream').Readable;
  assert(Readable);
} catch (e) {
  var Readable = require('readable-stream');
}

var m3u8 = require('./m3u8');

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

function getFileStream(srcUrl, options, cb) {
  assert(srcUrl.protocol);

  if (typeof options === 'function') {
    cb = options;
    options = {};
  }

  if (srcUrl.protocol === 'http:' || srcUrl.protocol === 'https:') {
    var headers = options.headers || {};
    if (!headers['user-agent']) headers['user-agent'] = DEFAULT_AGENT;
    if (!headers['accept-encoding']) headers['accept-encoding'] = ['gzip','deflate'];

    var req = (options.probe ? request.head : request.get)({url:url.format(srcUrl), pool:false, headers:headers});
    req.on('error', cb);
    req.on('response', function (res) {
      if (res.statusCode !== 200) {
        req.abort();
        return cb(new Error('Bad server response code: '+statusCode));
      }

      res.abort = req.abort.bind(req);
      // forward all errors to result
      req.on('error', function(err) {
        res.emit('error', err);
      });

      var stream = res;
      if (res.headers['content-encoding'] === 'gzip' || res.headers['content-encoding'] === 'deflate') {
        unzip = zlib.createUnzip();
        stream = res.pipe(unzip);
      }

      var typeparts = /^(.+?\/.+?)(?:;\w*.*)?$/.exec(res.headers['content-type']) || [null, 'application/octet-stream'],
          mimetype = typeparts[1].toLowerCase(),
          size = res.headers['content-length'] ? parseInt(res.headers['content-length'], 10) : -1,
          modified = res.headers['last-modified'] ? new Date(res.headers['last-modified']) : null;

      cb(null, stream, {url:res.url || url.format(srcUrl), mime:mimetype, size:size, modified:modified});
    });
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
        self.stream_start(true, !self.push(''));

        function Done(err) {
          debug('finished with input stream');

          stream.removeListener('error', Done);
          stream.removeListener('end', Done);
          stream.removeListener('close', Done);

          self.readState.stream = null;

          if (!err && (totalBytes !== meta.size))
            err = new Error('Invalid returned stream length (req='+meta.size+', ret='+totalBytes+')');

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

    if (typeof stream.abort == 'function') {
      var duration = self.readState.currentSegment.duration || self.index.target_duration || 10;
      self.readState.timer = setTimeout(function() {
        if (self.readState.stream) {
          debug('timed out waiting for data');
          self.readState.stream.abort();
        }
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
