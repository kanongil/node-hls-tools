var util = require('util'),
    url = require('url'),
    zlib = require('zlib'),
    assert = require('assert');

var request = require('request'),
    extend = require('xtend'),
    oncemore = require('./oncemore'),
    debug = require('debug')('hls:uristream');

try {
  var Readable = require('stream').Readable;
  assert(Readable);
} catch (e) {
  var Readable = require('readable-stream');
}

function noop() {};

var pkg = require('../package');
var DEFAULT_AGENT = util.format('%s/v%s (http://github.com/kanongil/node-hls-tools) node.js/%s', pkg.name, pkg.version, process.version);

module.exports = uristream;
uristream.UriFetchStream = UriFetchStream;
uristream.PartialError = PartialError;

function inheritErrors(stream) {
  stream.on('pipe', function(source) {
    source.on('error', stream.emit.bind(stream, 'error'));
  });
  stream.on('unpipe', function(source) {
    source.removeListener('error', stream.emit.bind(stream, 'error'));
  });
  return stream;
}

function setupHttp(uri, options, dst) {
  var defaults = {
    'user-agent': DEFAULT_AGENT,
    'accept-encoding': ['gzip','deflate']
  };

  // TODO: handle case in header names
  var headers = extend(defaults, options.headers);
  var timeout = options.timeout || 60*1000;
  var probe = !!options.probe;

  var retries = 10;
  var metaEmitted = false;
  var fetch = probe ? request.head : request.get;

  function fetchHttp(start) {
    if (start > 0)
      headers['range'] = 'bytes=' + start + '-';
    else
      delete headers['range'];

    var req = fetch({uri:uri, pool:false, headers:headers, timeout:timeout});
    req.on('error', onreqerror);
    req.on('error', noop);
    req.on('response', onresponse);

    function failOrRetry(err, temporary) {
      req.abort();
      dst.emit('error', err);
    }

    function reqcleanup() {
      req.removeListener('error', onreqerror);
      req.removeListener('response', onresponse);
    }

    function onreqerror(err) {
      reqcleanup();
      failOrRetry(err);
    }

    function onresponse(res) {
      reqcleanup();

      if (res.statusCode !== 200 && res.statusCode !== 206)
        return failOrRetry(new Error('Bad server response code: '+res.statusCode), res.statusCode >= 500 && res.statusCode !== 501);

      var size = res.headers['content-length'] ? parseInt(res.headers['content-length'], 10) : -1;

      // transparently handle gzip responses
      var stream = res;
      if (res.headers['content-encoding'] === 'gzip' || res.headers['content-encoding'] === 'deflate') {
        unzip = zlib.createUnzip();
        stream = stream.pipe(inheritErrors(unzip));
        size = -1;
      }

      // pipe it to self
      stream.on('data', function(chunk) {
        if (!dst.push(chunk))
          stream.pause();
      });
      oncemore(stream).once('end', 'error', function(err) {
        dst._read = noop;
        if (err) return failOrRetry(err);
        dst.push(null);
      });
      dst._read = function(n) {
        stream.resume();
      };

      // allow aborting the request
      dst.abort = req.abort.bind(req);

      // forward all future errors to response stream
      req.on('error', function(err) {
        if (dst.listeners('error').length !== 0)
          dst.emit('error', err);
      });

      // turn bad content-length into actual errors
      if (size >= 0 && !probe) {
        var accum = 0;
        res.on('data', function(chunk) {
          accum += chunk.length;
          if (accum > size)
            req.abort();
        });
      
        oncemore(res).once('end', 'error', function(err) {
          if (!err && accum !== size)
            failOrRetry(new PartialError('Stream length did not match header', accum, size), accum && accum < size);
        });
      }

      // attach empty 'error' listener to keep it from ever throwing
      dst.on('error', noop);

      if (!dst.meta) {
        // extract meta information from header
        var typeparts = /^(.+?\/.+?)(?:;\w*.*)?$/.exec(res.headers['content-type']) || [null, 'application/octet-stream'],
            mimetype = typeparts[1].toLowerCase(),
            modified = res.headers['last-modified'] ? new Date(res.headers['last-modified']) : null;

        dst.meta = {url:url.format(req.uri), mime:mimetype, size:start+size, modified:modified};
        dst.emit('meta', dst.meta);
      }
    }
  }

  fetchHttp(options.start || 0);
}

function UriFetchStream(uri, options) {
  var self = this;
  Readable.call(this, options);

  options = options || {};

  this.url = url.parse(uri);
  this.meta = null;

  if (this.url.protocol === 'http:' || this.url.protocol === 'https:') {
    setupHttp(uri, options, this);
  } else {
    throw new Error('Unsupported protocol: '+this.url.protocol);
  }
}
util.inherits(UriFetchStream, Readable);

UriFetchStream.prototype._read = noop;


function uristream(uri, options) {
  return new UriFetchStream(uri, options);
}

function PartialError(msg, processed, expected) {
  Error.captureStackTrace(this, this);
  this.message = msg || 'PartialError';
  this.processed = processed || -1;
  this.expected = expected;
}
util.inherits(PartialError, Error);
PartialError.prototype.name = 'Partial Error';
