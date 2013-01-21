#!/usr/bin/env node

var hlsmon = require('commander');
hlsmon.version('0.0.0')
   .usage('[options] <url>')
   .option('-a', '--user-agent <string>', 'User-Agent')
   .parse(process.argv);

var util = require('util'),
    url = require('url');

var reader = require('../lib/reader');

var src = process.argv[2];
var sep = ';';

function monitor(srcUrl) {
  var r = reader(srcUrl, {noData:true});

  var time = 0;
  r.on('segment', function(seqNo, duration, file) {
    console.log(file.modified.toJSON() + sep + file.size + sep + duration.toFixed(3) + sep + (file.size / (duration * 1024/8)).toFixed(3));
  //  console.error('new segment at '+time.toFixed(0)+' seconds, avg bitrate (kbps):', (file.size / (duration * 1024/8)).toFixed(1));
    time += duration;
  });

  r.on('end', function() {
    if (r.index.variant) {
      var newUrl = url.resolve(r.baseUrl, r.index.programs['1'][0].uri);
      console.error('found variant index, using: ', newUrl)
      return monitor(newUrl);
    }
    console.error('done');
  });

  r.resume();
}

monitor(src);