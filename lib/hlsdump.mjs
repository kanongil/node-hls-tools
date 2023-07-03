
import * as Fs from 'node:fs';
import { finished } from 'node:stream/promises';

import { UdpBlast } from 'udp-blast';

import { HlsReader } from '../lib/hls-reader.mjs';
import { createSimpleReader } from 'hls-segment-reader';


export async function run (src, options) {

    if (options.bufferSize) options.sync = true;

    const readerOptions = {
        highWaterMark: (options.concurrent || 1) - 1,
        fullStream: options.fullStream,
        startDate: Date(),
        lowLatency: options.lowLatency ?? true,
        onProblem(err) {

            if (!reader.isHooked) {
                throw err;
            }

            options.problem?.(err);
        }
    };

    const segmentReader = createSimpleReader(src, readerOptions);
    const reader = new HlsReader(segmentReader, options);

    try {
        const outputs = [];
        const addOutput = function (stream) {

            outputs.push(stream);
            reader.pipe(stream);

            // Output errors are fatal

            stream.on('error', (err) => reader.destroy(err));
        };

        if (options.udp) {
            const dst = (options.udp === true) ? null : options.udp;
            addOutput(new UdpBlast(dst, { packetSize: 7 * 188 }));
        }

        if (options.output) {
            if (options.output === '-')
                addOutput(process.stdout);
            else
                addOutput(Fs.createWriteStream(options.output));
        }

        options.started?.(reader, outputs);
    }
    catch (_err) {
        reader.destroy();
    }

    await finished(reader);
}
