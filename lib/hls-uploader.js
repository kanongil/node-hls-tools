'use strict';

const Assert = require('assert');
const Fs = require('fs');
const Path = require('path');
const Url = require('url');
const Util = require('util');

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

        Assert.equal(Url.parse(targetUri).protocol, null);

        this.targetUri = targetUri;

        this.indexName = options.indexName || 'index.m3u8';
        this.collect = !!options.collect;

        // State

        this.lastIndexString = '';
        this.segmentBytes = 0;

        // TODO: make async?
        if (!Fs.existsSync(this.targetUri)) {
            Mkdirp.sync(this.targetUri);
        }
    }

    async pushSegment(stream, name) {

        const target = this.prepareTargetStream(name);

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

    prepareTargetStream(name) {

        const append = this.collect && this.segmentBytes !== 0;
        return Fs.createWriteStream(Path.join(this.targetUri, name), { flags: append ? 'a' : 'w' });
    }

    flushIndex(index) {

        const indexString = index.toString().trim();

        let appendString;
        if (this.lastIndexString && indexString.startsWith(this.lastIndexString)) {
            const lastLength = this.lastIndexString.length;
            appendString = indexString.substr(lastLength);
        }
        this.lastIndexString = indexString;

        if (appendString) {
            return internals.fs.appendFile(Path.join(this.targetUri, this.indexName), appendString);
        }
        else {
            return internals.fs.writeFile(Path.join(this.targetUri, this.indexName), indexString);
        }
    }
};


module.exports = HlsUploader;
