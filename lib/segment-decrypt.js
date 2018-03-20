'use strict';

const Url = require('url');
const Crypto = require('crypto');

const Pati = require('pati');
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
};

internals.KeyFetcher.prototype.fetch = function () {

  let key = new Buffer(0);
  let headers = {};
  if (this.cookie) {
    headers.Cookie = this.cookie;
  }

  const dispatcher = new Pati.EventDispatcher(UriStream(this.uri, { headers: headers, whitelist: internals.allowedProtocols, timeout: internals.fetchTimeout }));

  dispatcher.on('data', (chunk) => {

    key = Buffer.concat([key, chunk]);
  });
  dispatcher.on('end', () => {

    dispatcher.end(key);
  });

  return dispatcher.finish();
};

internals.KeyFetcher.prototype.get = function () {

  if (!this.key) {
    this.key = this.fetch();
  }

  return this.key;
};


internals.fetchKey = function (keyUri, options) {

  if (options.key) {
    return options.key;
  }

  const uri = Url.resolve(options.base, keyUri);
  let fetcher = internals.keyCache[uri];
  if (!fetcher) {
    fetcher = internals.keyCache[uri] = new internals.KeyFetcher(uri, options.cookie);
  }

  return fetcher.get();
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


exports.decrypt = async function (stream, keyAttrs, options, next) {

  if (!keyAttrs || !options) {
    return stream;
  }

  let key = internals.getIdentityKey(keyAttrs);
  if (!key || key.method === 'NONE') {
    return stream;
  }

  if (key.method !== 'AES-128' || !key.uri || !key.iv) {

    // TODO: hard error when key is not recognized?
    throw new Error('unknown encryption parameters');
  }

  let keyData;
  try {
    keyData = await internals.fetchKey(key.uri, options);
  }
  catch (err) {
    throw new Error('key fetch failed: ' + (err.stack || err));
  }

  let decrypt;
  try {
    decrypt = Crypto.createDecipheriv('aes-128-cbc', keyData, key.iv);
  } catch (ex) {
    throw new Error('crypto setup failed: ' + (ex.stack || ex));
  }

  // forward stream errors
  stream.on('error', (err) => {

    decrypt.emit('error', err);
  });

  return stream.pipe(decrypt);
};
