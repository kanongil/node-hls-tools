import { Transform } from 'node:stream';

import { BufferList } from 'bl';

const parsePCR = function (buffer, pcr_pid) {

    const head = buffer.readUInt32BE(0);
    const pid = (head >> 8) & 0x1fff;
    if (((head >> 5) & 1) !== 1) return -1;
    if (pcr_pid && pcr_pid != pid) return -1;

    const s = buffer.readUInt8(4);
    if (s < 7) return -1;

    const f = buffer.readUInt8(5);
    if (((f >> 4) & 1) !== 1) return -1;

    let base = buffer.readUInt32BE(6) * 2;
    let ext = buffer.readUInt16BE(10);

    base += (ext >> 31);
    ext = ext & 0x1ff;

    return base / 0.09 + ext / 27;      // Return usecs
};


export const utime = function () {

    const t = process.hrtime();         // Based on CLOCK_MONOTONIC, and thus accommodates local drift (but apparently not suspend)
    return t[0] * 1E6 + t[1] / 1E3;
};


export class TimedChunk {

    constructor(start, end, size, buffers) {

        this.start = start;
        this.end = end;
        this.size = size;
        this.buffers = buffers;
    }
}

/**
 * Collect incoming stream data into timed chunks.
 */
export class TsCollect extends Transform {

    #incoming = new BufferList();
    #lastPCR = -1;

    constructor(options) {

        super({
            readableObjectMode: 1,
            readableHighWaterMark: 1 });

        //this.useResync = !!options?.useResync;
        //this.pcrPid = options.pcrPid;
    }

    _transform(chunk, _enconding, cb) {

        try {
            this.#transform(chunk);
        }
        catch (err) {
            return cb(err);
        }

        return cb();
    }

    #transform(chunk) {

        const start = Math.floor(this.#incoming.length / 188) * 188;

        this.#incoming.append(chunk);

        // Try to find PCR packet in newly added complete TS packets

        for (let i = start; i < this.#incoming.length - 187; i += 188) {
            const pcr = parsePCR(this.#incoming.slice(i, i + 12));
            if (pcr !== -1) {
                if (i !== 0) {

                    // Success

                    this.push(new TimedChunk(this.#lastPCR, pcr, i, this.#incoming.shallowSlice(0, i)._bufs));

                    // Reset to try remaining packet data

                    this.#incoming = this.#incoming.shallowSlice(i);
                    i = 0;
                }

                this.#lastPCR = pcr;
            }
        }
    }

    _flush(cb) {

        if (this.#incoming.length) {
            this.push(new TimedChunk(this.#lastPCR, undefined, this.#incoming));
            this.#incoming = new BufferList();
        }

        cb();
    }
}
