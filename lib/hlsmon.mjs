import { ContentFetcher } from 'hls-playlist-reader/helpers';
import { HlsPlaylistFetcher } from 'hls-playlist-reader/fetcher';
import { HlsSegmentReadable, HlsSegmentStreamer } from 'hls-segment-reader';
import { HlsSegmentFetcher } from 'hls-segment-reader/lib/segment-fetcher.js';


const contentBytes = (segmentInfo) => {

    if (segmentInfo.type === 'segment') {
        return segmentInfo.segment.entry.byterange ? +segmentInfo.segment.entry.byterange.length : segmentInfo.file.size;
    }

    if (segmentInfo.type === 'init') {
        return segmentInfo.init.byterange ? parseInt(segmentInfo.init.quotedString('byterange'), 10) : segmentInfo.file.size;
    }

    return segmentInfo.file.size;
};

const sep = ';';

const consoleReporter = (segmentInfo) => {

    const meta = segmentInfo.file;
    const size = contentBytes(segmentInfo);
    const duration = +(segmentInfo.segment && segmentInfo.segment.entry.duration);

    console.log(meta.modified.toJSON() + sep + size + sep + duration.toFixed(3) + sep + (size / (duration * 1024 / 8)).toFixed(3));
};

export async function monitor (srcUrl, { reporter = consoleReporter }) {

    const playlistFetcher = new HlsPlaylistFetcher(srcUrl, new ContentFetcher(), { lowLatency: false });
    const r = new HlsSegmentReadable(new HlsSegmentFetcher(playlistFetcher, { fullStream: true }));

    const { index } = await playlistFetcher.index();
    if (index && index.master) {
        const newUrl = new URL(index.variants[0].uri, playlistFetcher.baseUrl);
        console.error('found variant index, using:', newUrl.href);
        return monitor(newUrl);
    }

    const s = new HlsSegmentStreamer(r, { withData: false, highWaterMark: 3 });
    for await (const segmentInfo of s) {
        reporter(segmentInfo);
    }
};
