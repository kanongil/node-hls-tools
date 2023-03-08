
import * as Fs from 'node:fs';
import * as Http from 'node:http';
import { finished } from 'node:stream/promises';

import { default as Pati } from 'pati';
import { default as UdpBlast } from 'udp-blast';

import { HlsReader } from '../lib/hls-reader.mjs';
import { createSimpleReader } from 'hls-segment-reader';


export async function run (src, options) {

    if (options.bufferSize) options.sync = true;

    const readerOptions = {
        highWaterMark: (options.concurrent || 1) - 1,
        fullStream: options.fullStream,
        startDate: Date(),
        lowLatency: true,
        onProblem(err) {

            console.error('PROBLEM', err);
        }
    };

    const segmentReader = createSimpleReader(src, readerOptions);
    const reader = new HlsReader(segmentReader, options);
    const r = new Pati.EventDispatcher(reader);

    try {
        const addOutput = function (stream) {

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

        const startTime = process.hrtime();
        r.on('ready', () => {

            const delay = process.hrtime(startTime);
            console.error('"ready" after delay of ' + (delay[0] * 1e3 + delay[1] / 1e6).toFixed(2) + 'ms');
        });

        r.on('end', () => {

            r.end();
        });

        let totalDuration = 0;
        r.on('segment', (/** @type {import("hls-segment-reader").HlsStreamerObject} */segmentInfo) => {

            let downloadSize = 0;

            segmentInfo.stream.on('data', (chunk) => downloadSize += chunk.byteLength);

            // TODO: use a completed property instead

            finished(segmentInfo.stream)
                .catch(() => undefined)
                .then(() => {

                    const duration = segmentInfo.segment?.entry.duration ?? 0;

                    totalDuration += duration;

                    console.error('segment done at ' + totalDuration.toFixed(0) + ' seconds, avg bitrate (kbps):', (downloadSize / (duration * 1024 / 8)).toFixed(1));
                });
        });

        if (options.infoPort) {
            const stats = (await import('measured-core')).createCollection();
            let currentSegment = -1;

            // setup stat tracking
            stats.gauge('bufferBytes', () => reader.buffer._readableState.length/* + buffer._writableState.length*/);
            stats.gauge('currentSegment', () => currentSegment);
            stats.gauge('index.first', () => segmentReader.fetcher.source.playlist.index ? segmentReader.fetcher.source.playlist.index.media_sequence : -1);
            stats.gauge('index.last', () => segmentReader.fetcher.source.playlist.index ? segmentReader.fetcher.source.playlist.index.lastMsn() : -1);
            stats.gauge('totalDuration', () => totalDuration);

            stats.timer('fetchTime').unref();
            stats.meter('streamErrors').unref();

            r.on('segment', (/** @type {import("hls-segment-reader").HlsStreamerObject} */segmentInfo) => {

                currentSegment = segmentInfo.segment && segmentInfo.segment.msn;

                const stopwatch = stats.timer('fetchTime').start();

                finished(segmentInfo.stream)
                    .catch(() => {

                        stats.meter('streamErrors').mark();
                    })
                    .finally(() => stopwatch.end());
            });

            const server = Http.createServer((req, res) => {

                if (req.method === 'GET') {
                    const data = JSON.stringify(stats, null, ' ');
                    res.writeHead(200, {
                        'Content-Type': 'application/json',
                        'Content-Length': data.length
                    });
                    res.write(data);
                }

                res.end();
            }).listen(options.infoPort);

            const cleanup = () => {

                server.close();
            };

            r.finish().finally(cleanup);
        }
    }
    catch (_err) {
        reader.destroy();
    }

    return r.finish();
}
