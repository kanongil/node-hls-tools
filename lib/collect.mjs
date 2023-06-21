import { Transform } from 'node:stream';

import { BufferList } from 'bl';

import { TimeStamp } from './utils.mjs';

// In Transport Streams the intended rate is determined by the values of the PCR fields and the number of Transport Stream bytes between them. (ISO-13818-1 D.0.9)

class PcrTimeStamp extends TimeStamp {

    constructor(value = 0n) {

        super(value, 27_000_000n);
    }
}

/**
 * Object containing collected buffers between 2 PCR TS packets.
 */
export class TimedChunk {

    /**
     * @param {TimeStamp | undefined} start 
     * @param {TimeStamp | undefined} end 
     * @param {number} size 
     * @param {BufferList} buffers 
     * @param {Date | number | undefined} date - Expected output date of first buffer byte
     */
    constructor(start, end, size, buffers, date) {

        this.start = start;
        this.end = end;
        this.size = size;
        this.buffers = buffers;

        this.date = date ? +date : undefined;
    }
}

const PCR_CYCLE_TIME = new PcrTimeStamp(1n << 33n);

const parsePCR = function (buffer, pcr_pid) {

    const head = buffer.readUInt32BE(0);
    const pid = (head >> 8) & 0x1fff;
    if (((head >> 5) & 1) !== 1) return undefined;
    if (pcr_pid && pcr_pid != pid) return undefined;

    const s = buffer.readUInt8(4);
    if (s < 7) return undefined;

    const f = buffer.readUInt8(5);
    if (((f >> 4) & 1) !== 1) return undefined;

    let base = buffer.readUInt32BE(6) * 2;
    let ext = buffer.readUInt16BE(10);

    base += (ext >> 31);
    ext = ext & 0x1ff;

    return new PcrTimeStamp(base * 300 + ext);
};

/**
 * Collect incoming stream data into timed chunks.
 */
export class TsCollect extends Transform {

    /** Time of very first byte. @type {number | undefined} */
    startTime;

    maxPcrInterval = 100;                // 100 ms per MPEG2-TS spec
    maxChunkSize = 2 * 1024 * 1024;      // 2 MB = ~160Mbps with 100 ms interval

    #incoming = new BufferList();
    #timeOffset = new PcrTimeStamp();

    /** @type {TimeStamp | undefined} */
    #lastTime = undefined;

    constructor(options) {

        super({
            readableObjectMode: 1,
            readableHighWaterMark: 1 });

        //this.useResync = !!options?.useResync;
        this.pcrPid = options?.pcrPid;
        this.startTime = options?.startTime;
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

        if (this.#incoming.length > this.maxChunkSize) {
            throw new Error(`Input stream error: No PCR packet in the last ${this.maxChunkSize} bytes`);
        }

        // Try to find PCR packet in newly added complete TS packets

        for (let i = start; i < this.#incoming.length - 187; i += 188) {
            const time = this.#getPacketTime(this.#incoming.slice(i, i + 12));
            if (time !== undefined) {
                if (i !== 0) {
                    if (this.#lastTime) {
                        const elapsed = time.subtract(this.#lastTime);

                        if (elapsed?.valueOf() > this.maxPcrInterval) {
                            this.emit('warning', new Error('PCR_error: ' + (elapsed / 1000).toFixed(2) + 's missing'));
                        }
                    }

                    // Success

                    this.push(new TimedChunk(this.#lastTime, time, i, this.#incoming.shallowSlice(0, i), this.startTime));
                    this.startTime = undefined;

                    // Reset to try remaining packet data

                    this.#incoming = this.#incoming.shallowSlice(i);
                    i = 0;
                }

                this.#lastTime = time;
            }
        }
    }

    _flush(cb) {

        if (this.#incoming.length) {
            this.push(new TimedChunk(this.#lastTime, undefined, this.#incoming));
            this.#incoming = new BufferList();
        }

        cb();
    }

    /**
     * Returns monotonic packet time.
     * 
     * Uses internal state and requires each provided buffer to be further in the stream than the previous.
     *
     * @param {Buffer} buffer - must always be further in the stream than previous buffer
     * @return {TimeStamp}
     */
    #getPacketTime(buffer) {

        const pcr = parsePCR(buffer, this.pcrPid);
        if (pcr === undefined) {
            return undefined;
        }

        let pcrTime = pcr.add(this.#timeOffset);

        // Handle timestamp wraparound

        if (this.#lastTime && !pcrTime.greaterThan(this.#lastTime)) {
            this.#timeOffset = this.#timeOffset.add(PCR_CYCLE_TIME);
            pcrTime = pcrTime.add(PCR_CYCLE_TIME);
        }

        return pcrTime;
    }
}
