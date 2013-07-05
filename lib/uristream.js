"use strict";

var util = require('util'),
    url = require('url'),
    zlib = require('zlib'),
    assert = require('assert');

var request = require('request'),
    extend = require('xtend'),
    equal = require('deep-equal'),
    oncemore = require('oncemore'),
    debug = require('debug')('hls:uristream');

try {
  var Readable = require('stream').Readable;
  assert(Readable);
} catch (e) {
  var Readable = require('readable-stream');
}

function noop() {};

var pkg = require('../package');
var DEFAULT_AGENT = util.format('%s/v%s (http://github.com/kanongil/node-hls-tools) request/v%s node.js/%s', pkg.name, pkg.version, require('request/package').version, process.version);

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

// 'pipe' any stream to a Readable
function pump(src, dst, done) {
  src.on('data', function(chunk) {
    if (!dst.push(chunk))
      src.pause();
  });
  oncemore(src).once('end', 'error', function(err) {
    dst._read = noop;
    done(err);
  });
  dst._read = function(n) {
    src.resume();
  };
}

function setupHttp(uri, options, dst) {
  var defaults = {
    'user-agent': DEFAULT_AGENT
  };

  // TODO: handle case in header names
  var timeout = options.timeout || 10*1000;
  var probe = !!options.probe;
  var offset = ~~options.start;
  var agent = options.agent || null;

  var tries = 10;
  if (!probe) defaults['accept-encoding'] = ['gzip','deflate'];

  var fetch = probe ? request.head : request.get;
  var headers = extend(defaults, options.headers);
  if ('range' in headers)
    throw new Error('range header is not allowed');

  // attach empty 'error' listener to keep dst from ever throwing
  dst.on('error', noop);

  function fetchHttp(start) {
    if (start > 0)
      headers['range'] = 'bytes=' + start + '-';

    var accum = 0, size = -1;
    var req = fetch({uri:uri, headers:headers, agent:agent, timeout:timeout});
    req.on('error', onreqerror);
    req.on('response', onresponse);

    var failed = false;
    function failOrRetry(err, temporary) {
      if (failed) return;
      failed = true;

      req.abort();
      if (--tries <= 0) {
        // remap error to partial error if we have received any data
        if (start - offset + accum !== 0)
          err = new PartialError(err, start - offset + accum, (size !== -1) ? start - offset + size : size);
        return dst.emit('error', err);
      }
      debug('retrying at ' + (start + accum));

      // TODO: delay retry?
      fetchHttp(start + accum);
    }

    function reqcleanup() {
      req.removeListener('error', onreqerror);
      req.removeListener('response', onresponse);
      req.on('error', noop);
    }

    function onreqerror(err) {
      reqcleanup();
      failOrRetry(err);
    }

    function onresponse(res) {
      reqcleanup();

      if (res.statusCode !== 200 && res.statusCode !== 206)
        return failOrRetry(new Error('Bad server response code: '+res.statusCode), res.statusCode >= 500 && res.statusCode !== 501);

      if (res.headers['content-length']) size = parseInt(res.headers['content-length'], 10);
      var filesize = (size >= 0) ? start + size : -1;

      // transparently handle gzip responses
      var stream = res;
      if (res.headers['content-encoding'] === 'gzip' || res.headers['content-encoding'] === 'deflate') {
        unzip = zlib.createUnzip();
        stream = stream.pipe(inheritErrors(unzip));
        filesize = -1;
      }

      // pipe it to self
      pump(stream, dst, function(err) {
        if (err || failed) return failOrRetry(err);
        debug('done fetching uri', uri);
        dst.push(null);

        dst.closed = true;
        dst.emit('close');
      });

      // allow aborting the request
      dst.abort = function(reason) {
        if (!dst.closed) {
          tries = 0;
          req.abort();
        }
      }

      // forward all future errors to response stream
      req.on('error', function(err) {
        if (dst.listeners('error').length !== 0)
          dst.emit('error', err);
      });

      // turn bad content-length into actual errors
      if (size >= 0 && !probe) {
        res.on('data', function(chunk) {
          accum += chunk.length;
          if (accum > size)
            req.abort();
        });

        oncemore(res).once('end', 'error', function(err) {
          if (!err && accum !== size)
            failOrRetry(new Error('Stream length did not match header'), accum && accum < size);
        });
      }

      // extract meta information from header
      var typeparts = /^(.+?\/.+?)(?:;\w*.*)?$/.exec(res.headers['content-type']) || [null, 'application/octet-stream'],
          mimetype = typeparts[1].toLowerCase(),
          modified = res.headers['last-modified'] ? new Date(res.headers['last-modified']) : null;

      var meta = { url:url.format(req.uri), mime:mimetype, size:filesize, modified:modified };
      if (dst.meta) {
        if (!equal(dst.meta, meta)) {
          tries = 0;
          failOrRetry(new Error('File has changed'));
        }
      } else  {
        dst.meta = meta;
        dst.emit('meta', dst.meta);
      }
    }
  }

  fetchHttp(offset);
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

function PartialError(err, processed, expected) {
  Error.call(this);

  if (err.stack) {
    Object.defineProperty(this, 'stack', {
      enumerable: false,
      configurable: false,
      get: function() { return err.stack; }
    });
  }
  else Error.captureStackTrace(this, arguments.callee);

  this.message = err.message || err.toString();
  this.processed = processed || -1;
  this.expected = expected;
}
util.inherits(PartialError, Error);
PartialError.prototype.name = 'Partial Error';
