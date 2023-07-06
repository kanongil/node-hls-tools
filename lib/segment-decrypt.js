'use strict';

const Crypto = require('crypto');

const UriStream = require('uristream');


const internals = {
    allowedProtocols: ['http', 'https', 'data'],
    fetchTimeout: 10 * 1000,

    keyCache: {},
};


internals.KeyFetcher = class {

    constructor(uri, cookie) {

        this.uri = uri;
        this.cookie = cookie;
        this.key = null;
    }

    async fetch() {

        let key = Buffer.alloc(0);
        let headers = {};
        if (this.cookie) {
            headers.Cookie = this.cookie;
        }

        const stream = UriStream(this.uri, { headers: headers, whitelist: internals.allowedProtocols, timeout: internals.fetchTimeout });
        for await (const chunk of stream) {
            key = Buffer.concat([key, chunk]);
        }

        return key;
    }

    async get() {

        if (!this.key) {
            this.key = await this.fetch();
        }

        return this.key;
    }
};


internals.fetchKey = function (keyUri, options) {

    if (options.key) {
        return options.key;
    }

    const uri = new URL(keyUri, options.base).href;
    let fetcher = internals.keyCache[uri];
    if (!fetcher) {
        fetcher = internals.keyCache[uri] = new internals.KeyFetcher(uri, options.cookie);
    }

    return fetcher.get();
};


internals.getIdentityKey = function (methods, keyAttrs) {

    for (const key of keyAttrs) {
        const keyformat = key.get('keyformat', 'string');
        const keymethod = key.get('method');
        if (!(keyformat || keyformat === 'identity') && 
                (keymethod === 'NONE' || keymethod === methods.has(keyformat) !== -1)) {

            return {
                method: keymethod,
                uri: key.has('uri') ? key.get('uri', 'string') : undefined,
                iv: key.has('iv') ? key.get('iv', 'hexint') : undefined
            };
        }
    }

    return null;
};

exports.methods = new Map([
    ['AES-128', (keyData, iv) => Crypto.createDecipheriv('aes-128-cbc', keyData, iv)]
]);

exports.decrypt = async function (stream, keyAttrs, options) {

    if (!keyAttrs || !options) {
        return stream;
    }

    const key = internals.getIdentityKey(new Set(...exports.methods.keys()), keyAttrs);
    const decryptor = exports.methods.get(key?.method);
    if (!decryptor) {
        return stream;
    }

    if (!key.uri || key.iv === undefined) {
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
        // Convert IV to Buffer

        const iv = Buffer.alloc(16);
        let work = key.iv;
        for (let i = 15; i >= 0 && work !== 0n; --i) {
            iv.writeUInt8(Number(work & 0xffn), i);
            work = work >> 8n;
        }

        decrypt = decryptor(keyData, iv);
    } catch (ex) {
        throw new Error('crypto setup failed: ' + (ex.stack || ex));
    }

    // Forward stream errors

    stream.on('error', (err) => {

        decrypt.emit('error', err);
    });

    return stream.pipe(decrypt);
};
