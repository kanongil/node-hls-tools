// stream from hls source

var util = require('util'),
    url = require('url'),
    assert = require('assert');

var async = require('async'),
    http = require('http-get'),
    debug = require('debug')('hls:reader'),
    Readable = require('readable-stream'),
    Transform = require('readable-stream/transform');

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

function getFileStream(srcUrl, options, cb) {
  assert(srcUrl.protocol);

  if (typeof options === 'function') {
    cb = options;
    options = {};
  }

  if (srcUrl.protocol === 'http:' || srcUrl.protocol === 'https:') {
    var headers = options.headers || {};
    if (!headers['user-agent']) headers['user-agent'] = DEFAULT_AGENT;

    (options.probe ? http.head : http.get)({url:url.format(srcUrl), stream:true, headers:headers}, function(err, res) {
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

      if (res.stream)
        res.stream.resume(); // for some reason http-get pauses the stream for the callback
      cb(null, res.stream, {url:res.url || srcUrl, mime:mimetype, size:size, modified:modified});
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
  Transform.call(this, options);

  if (typeof src === 'string')
    src = url.parse(src);

  this.url = src;
  this.baseUrl = src;
  this.options = options || {};

  this.indexStream = null;
  this.index = null;

  this.readState = {
    currentSeq:-1,
    currentSegment:null,
    readable:null
  }

  function updateindex() {
    getFileStream(self.url, function(err, stream, meta) {
      if (err) return self.emit('error', err);

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

        if (updated) {
          if (self.readState.currentSeq===-1)
            self.readState.currentSeq = index.startSeqNo();

          self.emit('index', index);

          if (index.variant)
            return self.end();

          if (!self.readState.currentSegment)
            checkcurrent();
        }

        if (!index.ended) {
          var updateInterval = updated ? index.segments[index.segments.length-1].duration : self.index.target_duration / 2;
          debug('scheduling index refresh', updateInterval);
          setTimeout(updateindex, Math.max(1, updateInterval)*1000);
        }
      });
    });
  }
  updateindex();

  function checkcurrent() {
    self.readState.currentSegment = self.index.getSegment(self.readState.currentSeq);
    if (self.readState.currentSegment)
      fetchfrom(self.readState.currentSegment);
    else if (self.index.ended)
      self.end();
    else if (!self.index.type && (self.index.lastSeqNo() < self.readState.currentSeq-1)) {
      // handle live stream restart
      self.readState.currentSeq = self.index.first_seq_no;
      checkcurrent();
    }
  }

  function fetchfrom(segment) {
    var segmentUrl = url.resolve(self.baseUrl, segment.uri)

    debug('fetching segment', segmentUrl);
    getFileStream(url.parse(segmentUrl), {probe:!!self.options.noData}, function(err, stream, meta) {
      if (err) return self.emit('error', err);

      if (meta.mime !== 'video/mp2t'/* && 
          meta.mime !== 'audio/aac' && meta.mime !== 'audio/x-aac' &&
          meta.mime !== 'audio/ac3'*/)
        return self.emit('error', new Error('Unsupported segment MIME type: '+meta.mime));

      self.emit('segment', self.readState.currentSeq, segment.duration, meta);

      function nextstream() {
        self.readState.currentSeq++;
        checkcurrent();
      }

      if (stream) {
        var r = stream;
        if (!(stream instanceof Readable)) {
          r = new Readable();
          r.wrap(stream);
        }
        self.readState.readable = r;
        r.pipe(self, {end:false});

        r.on('end', function() {
          r.unpipe(self);
          process.nextTick(nextstream);
        });
      } else {
        process.nextTick(nextstream);
      }
    });
  }

  return this;
}
util.inherits(HlsStreamReader, Transform);

HlsStreamReader.prototype._transform = function(chunk, output, cb) {
  // TODO: decrypt here
  cb(null, chunk);
};

function hlsreader(url, options) {
  return new HlsStreamReader(url, options);
}
