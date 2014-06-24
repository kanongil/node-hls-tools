"use strict";

var dgram = require('dgram'),
    util = require('util'),
    assert = require('assert');

var Writable = require('readable-stream/writable');

function TsBlast(dst, options) {
  Writable.call(this, options);

  if (typeof dst === 'number')
    dst = {port:dst, host:'localhost'};

  this.dst = dst;
  this.options = options || {};

  this.buffer = new Buffer(0);
  this.client = dgram.createSocket('udp4');
  this.client.bind();

  this.on('finish', function() {
    this.client.close();
  });

  return this;
}
util.inherits(TsBlast, Writable);

TsBlast.prototype._write = function(chunk, encoding, cb) {
  var self = this;

  if (chunk) {
    if (this.buffer.length)
      this.buffer = Buffer.concat([this.buffer, chunk]);
    else
      this.buffer = chunk;
  }

  var index = 0, psize = 188 * 7;

  function sendnext() {
    if ((self.buffer.length - index) >= psize) {
      self.client.send(self.buffer, index, psize, self.dst.port, self.dst.host, function(/*err, bytes*/) {
        // TODO: handle errors?
        index += psize;
        sendnext();
      });
    } else {
      /*    if (!chunk) {
            self.client.send(self.buffer, index, self.buffer.length - index, self.dst.port, self.dst.host);
            index = self.buffer.length;
          }*/
      if (index) self.buffer = self.buffer.slice(index);
      cb();
    }
  }

  sendnext();
};


var tsblast = module.exports = function tsblast(dst, options) {
  return new TsBlast(dst, options);
};

tsblast.TsBlast = TsBlast;
