"use strict";

var Url = require('url');
var Crypto = require('crypto');

var Oncemore = require('oncemore');
var UriStream = require('uristream');


var internals = {
  allowedProtocols: ['http', 'https', 'data'],
  fetchTimeout: 10 * 1000,

  keyCache: {},
};


internals.KeyFetcher = function (uri, cookie) {
  this.uri = uri;
  this.cookie = cookie;
  this.key = null;

  this._gets = [];
};

internals.KeyFetcher.prototype.fetch = function (next) {
  var key = new Buffer(0);
  var headers = {};
  if (this.cookie) {
    headers.Cookie = this.cookie;
  }

  Oncemore(UriStream(this.uri, { headers: headers, whitelist: internals.allowedProtocols, timeout: internals.fetchTimeout }))
    .on('data', function(chunk) {
      key = Buffer.concat([key, chunk]);
    })
    .once('error', 'end', function(err) {
      return next(err, key);
    });
};

internals.KeyFetcher.prototype.get = function (next) {
  var self = this;

  if (this.key && this.key.length) {
    return next(null, this.key);
  }

  var complete = function (err, key) {
    if (!err) {
      self.key = key;
    }

    var gets = self._gets;
    self._gets = [];

    for (var idx = 0; idx < gets.length; idx++) {
      var _next = gets[idx];
      process.nextTick(function() {
        _next(err, key);
      });
    }
  };

  if (this._gets.length === 0) {
    this.fetch(complete);
  }

  return this._gets.push(next);
};


internals.fetchKey = function (keyUri, options, next) {
  if (options.key) return next(null, options.key);

  var uri = Url.resolve(options.base, keyUri);
  var fetcher = internals.keyCache[uri];
  if (!fetcher) {
    fetcher = internals.keyCache[uri] = new internals.KeyFetcher(uri, options.cookie);
  }
  return fetcher.get(next);
};


exports.decrypt = function (stream, keyAttrs, options, next) {
  var method = keyAttrs && keyAttrs.enumeratedString('method');
  if (!method || method === 'NONE') {
    return next(null, stream);
  }

  if (method !== 'AES-128' || !keyAttrs.quotedString('uri') || !keyAttrs.hexadecimalInteger('iv')) {

    // TODO: hard error when key is not recognized?
    return next(new Error('unknown encryption parameters'), stream);
  }

  return internals.fetchKey(keyAttrs.quotedString('uri'), options, function(err, key) {
    if (err) {
      return next(new Error('key fetch failed: ' + (err.stack || err)));
    }

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
