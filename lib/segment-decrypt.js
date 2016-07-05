'use strict';

const Url = require('url');
const Crypto = require('crypto');

const Oncemore = require('oncemore');
const UriStream = require('uristream');


const internals = {
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

  let key = new Buffer(0);
  let headers = {};
  if (this.cookie) {
    headers.Cookie = this.cookie;
  }

  Oncemore(UriStream(this.uri, { headers: headers, whitelist: internals.allowedProtocols, timeout: internals.fetchTimeout }))

    .on('data', (chunk) => {

      key = Buffer.concat([key, chunk]);
    })
    .once('error', 'end', (err) => {

      return next(err, key);
    });
};

internals.KeyFetcher.prototype.get = function (next) {

  if (this.key && this.key.length) {
    return next(null, this.key);
  }

  const complete = (err, key) => {

    if (!err) {
      this.key = key;
    }

    let gets = this._gets;
    this._gets = [];

    for (let idx = 0; idx < gets.length; idx++) {
      process.nextTick(gets[idx], err, key);
    }
  };

  if (this._gets.length === 0) {
    this.fetch(complete);
  }

  return this._gets.push(next);
};


internals.fetchKey = function (keyUri, options, next) {

  if (options.key) return next(null, options.key);

  let uri = Url.resolve(options.base, keyUri);
  let fetcher = internals.keyCache[uri];
  if (!fetcher) {
    fetcher = internals.keyCache[uri] = new internals.KeyFetcher(uri, options.cookie);
  }
  return fetcher.get(next);
};


internals.getIdentityKey = function (keyAttrs) {

  for (let idx = 0; idx < keyAttrs.length; idx++) {
    let key = keyAttrs[idx];
    let keyformat = key.quotedString('keyformat');
    if (!keyformat || keyformat === 'identity') {
      return {
        method: key.enumeratedString('method'),
        uri: key.quotedString('uri'),
        iv: key.hexadecimalInteger('iv')
      };
    }
  }

  return null;
};


exports.decrypt = function (stream, keyAttrs, options, next) {

  if (!keyAttrs || !options) {
    return next(null, stream, false);
  }

  let key = internals.getIdentityKey(keyAttrs);
  if (!key || key.method === 'NONE') {
    return next(null, stream, false);
  }

  if (key.method !== 'AES-128' || !key.uri || !key.iv) {

    // TODO: hard error when key is not recognized?
    return next(new Error('unknown encryption parameters'), stream);
  }

  return internals.fetchKey(key.uri, options, (err, keyData) => {

    if (err) {
      return next(new Error('key fetch failed: ' + (err.stack || err)));
    }

    let decrypt;
    try {
      decrypt = Crypto.createDecipheriv('aes-128-cbc', keyData, key.iv);
    } catch (ex) {
      return next(new Error('crypto setup failed: ' + (ex.stack || ex)));
    }

    // forward stream errors
    stream.on('error', (err) => {

      decrypt.emit('error', err);
    });

    return next(null, stream.pipe(decrypt), true);
  });
};
