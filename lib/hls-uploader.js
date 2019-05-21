'use strict';

const Assert = require('assert');
const Fs = require('fs');
const Path = require('path');
const Url = require('url');
const Util = require('util');
const debug = require('debug')('hls:uploader');

const Aws = require('aws-sdk');
const Mkdirp = require('mkdirp');
const Pati = require('pati');
const WriteFileAtomic = require('write-file-atomic');


const internals = {};


internals.fs = {
    appendFile: Util.promisify(Fs.appendFile),
    writeFile: Util.promisify(WriteFileAtomic)
};


class HlsUploader {

    constructor(targetUri, options) {

        const url = new URL(targetUri);
        Assert.ok(url.protocol === 'file:' || url.protocol === 's3:');

        this.indexName = options.indexName || 'index.m3u8';
        this.collect = !!options.collect;
        this.cacheDuration = options.cacheDuration || 7 * 24 * 3600 * 1000;

        // State

        this.lastIndexString = '';
        this.segmentBytes = 0;

        if (url.protocol === 's3:') {
            Assert.equal(options.collect, false, 'Collect not supported with s3:');

            const params = {
                params: {
                    Bucket: url.host,
                    ACL: 'public-read',
                    StorageClass: 'REDUCED_REDUNDANCY'
                }
            };

            this.s3 = new Aws.S3(params);
            this.baseKey = (url.pathname || '/').slice(1);
        } else {
            this.targetPath = Url.fileURLToPath(url);

            // TODO: make async?
            if (!Fs.existsSync(this.targetPath)) {
                Mkdirp.sync(this.targetPath);
            }
        }
    }

    async pushSegment(stream, name, meta) {

        const append = this.collect && this.segmentBytes !== 0;

        if (this.s3) {
            const params = {
                Body: stream,
                Key: Path.join(this.baseKey, name),
                ContentType: meta.mime || 'video/MP2T',
                CacheControl: `max-age=300, s-max-age=${Math.floor(this.cacheDuration / 1000)}, public`,
                ContentLength: meta.size
            };

            return new Promise((resolve, reject) => {

                this.s3.upload(params, (err, data) => {

                    debug('upload finished for', Path.join(this.baseKey, name), err);
                    return err ? reject(err) : resolve(data);
                });
            });
        }

        const target = Fs.createWriteStream(Path.join(this.targetPath, name), { flags: append ? 'a' : 'w' });
        stream.pipe(target);

        const dispatcher = new Pati.EventDispatcher(stream);

        dispatcher.on('end', Pati.EventDispatcher.end);

        let bytesWritten = 0;
        dispatcher.on('data', (chunk) => {

            bytesWritten += +chunk.length;
        });

        try {
            // TODO: handle target errors & wait for end?
            await dispatcher.finish();
            return bytesWritten;
        }
        finally {
            this.segmentBytes += bytesWritten;
        }
    }

    async flushIndex(index) {

        const indexString = index.toString().trim();

        if (this.s3) {
            const cacheTime = index.ended ? this.cacheDuration : index.target_duration * 1000 / 2;

            const params = {
                Body: indexString,
                Key: Path.join(this.baseKey, this.indexName),
                ContentType: 'application/vnd.apple.mpegURL',
                CacheControl: `max-age=${Math.floor(cacheTime / 1000)}, public`
            };

            return new Promise((resolve, reject) => {

                this.s3.putObject(params, (err, data) => {

                    return err ? reject(err) : resolve(data);
                });
            });
        }

        let appendString;
        if (this.lastIndexString && indexString.startsWith(this.lastIndexString)) {
            const lastLength = this.lastIndexString.length;
            appendString = indexString.substr(lastLength);
        }
        this.lastIndexString = indexString;

        if (appendString) {
            return internals.fs.appendFile(Path.join(this.targetPath, this.indexName), appendString);
        }
        else {
            return internals.fs.writeFile(Path.join(this.targetPath, this.indexName), indexString);
        }
    }
};


module.exports = HlsUploader;
