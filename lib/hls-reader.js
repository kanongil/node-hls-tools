"use strict";

var Url = require('url'),
    Util = require('util'),
    Crypto = require('crypto');

var StreamProcess = require('streamprocess'),
    oncemore = require('oncemore'),
    UriStream = require('uristream');

var Readable = require('readable-stream/readable'),
    Passthrough = require('readable-stream/passthrough');

var tssmooth = require('./tssmooth');

var internals = {
  keyCache: {},
};

var NOOP = function(){};

// 'pipe' stream to a Readable
function pump(src, dst, done) {
  src.on('data', function(chunk) {
    if (!dst.push(chunk)) {
      src.pause();
    }
  });
  oncemore(src).once('end', 'error', function(err) {
    // TODO: flush source buffer on error?
    dst._read = NOOP;
    done(err);
  });
  dst._read = function() {
    src.resume();
  };
}

function HlsReader(segmentReader, options) {
  if (!(this instanceof HlsReader))
    return new HlsReader(segmentReader, options);

  Readable.call(this, { lowWaterMark: options.lowWaterMark, highWaterMark: options.highWaterMark });

  var self = this;

  this.reader = segmentReader;

  this.sync = !!options.sync; // output in real-time
  this.bufferSize = ~~options.bufferSize;

  this.cookie = options.cookie;
  this.key = options.key;

  if (options.key && !Buffer.isBuffer(options.key) && options.key.length !== 32) {
    throw new TypeError('key must be a 32 byte Buffer');
  }

  this.isReading = false;
  this.isHooked = false;
  this.buffer = new Passthrough({ highWaterMark: this.bufferSize });

  StreamProcess(this.reader, function (obj, done) {
    self.isReading = true;

    return self.decrypt(obj.stream, obj.segment.key, function (err, stream) {
      if (err) {
        console.error('decrypt failed', err.stack);
        stream = obj.stream;
      }

      self.emit('segment', obj);

      stream = oncemore(stream);

      if (!self.isHooked) {
        // pull data and detect if we need to hook before end
        var buffered = 0;
        stream.on('data', function(chunk) {
          buffered += chunk.length;
          if (!self.isHooked && buffered >= self.bufferSize)
            self.hook();
        });
      }

      stream.pipe(self.buffer, { end: false });
      stream.once('end', 'error', function(err) {
        self.isReading = false;
        if (err) {
          console.error('stream error', err.stack || err);
        }
        self.hook();
        done();
      });
    });
  });

  this.reader.once('index', function() {
    // wait until first index is returned before attaching error listener.
    // this will enable initials errors to throw
    self.reader.on('error', function(err) {
      console.error('reader error', err.stack || err);
    });
  });

  this.reader.on('end', function() {
    console.error('done');
    self.buffer.end();
  });

  // start output if needed
  if (!this.sync || !(this.bufferSize > 0))
    this.hook();
}
Util.inherits(HlsReader, Readable);

// the hook is used to prebuffer
HlsReader.prototype.hook = function hook() {
  var self = this;

  if (this.isHooked) return;

  console.error('hooking output', this.sync);
  self.isHooked = true;

  var s = this.buffer;
  if (this.sync) {
    var smooth = tssmooth();
    smooth.on('unpipe', function() {
      this.unpipe();
    });
    smooth.on('warning', function(err) {
      console.error('smoothing error', err);
    });
    s = s.pipe(smooth);
  }

  pump(s, this, function(err) {
    if (err) {
      return self.emit('error', err);
    }
    self.push(null);
  });
};

HlsReader.prototype.decrypt = function (stream, keyAttrs, next) {
  if (!keyAttrs) return next(null, stream);

  if (keyAttrs.enumeratedString('method') !== 'AES-128' ||
      !keyAttrs.quotedString('uri') || !keyAttrs.hexadecimalInteger('iv')) {

    // TODO: hard error when key is not recognized?
    return next(new Error('unknown encryption parameters'));
  }

  return this.fetchKey(keyAttrs.quotedString('uri'), function(err, key) {
    if (err)
      return next(new Error('key fetch failed: ' + (err.stack || err)));

    var iv = keyAttrs.hexadecimalInteger('iv');
    try {
      var decrypt = Crypto.createDecipheriv('aes-128-cbc', key, iv);
    } catch (ex) {
      return next(new Error('crypto setup failed: ' (ex.stack || ex)));
    }

    // forward stream errors
    stream.on('error', function(err) {
      decrypt.emit('error', err);
    });

    return next(null, stream.pipe(decrypt));
  });
};

HlsReader.prototype.fetchKey = function (keyUri, next) {
  if (this.key) return next(null, this.key);

  var uri = Url.resolve(this.reader.url, keyUri);
  var entry = internals.keyCache[uri];
  if (entry && entry.length) return next(null, internals.keyCache[uri]);

  var key = new Buffer(0);
  var headers = {};
  if (this.cookie)
    headers.Cookie = this.cookie;

  oncemore(UriStream(uri, { headers: headers, whitelist: ['http', 'https', 'data'], timeout: 10 * 1000 }))
    .on('data', function(chunk) {
      key = Buffer.concat([key, chunk]);
    })
    .once('error', 'end', function(err) {
      internals.keyCache[uri] = key;
      return next(err, key);
    });
};

var hlsreader = module.exports = function hlsreader(segmentReader, options) {
  return new HlsReader(segmentReader, options);
};

hlsreader.HlsReader = HlsReader;
