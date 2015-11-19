'use strict';

var extend = require('util')._extend;
var util = require('util');

var EventEmitter = require('events').EventEmitter;
var Transform = require('stream').Transform;

var RRD_COOKIE = 'RRD';
var RRD_VERSION = '0003';
var FLOAT_COOKIE = 8.642135e+130;

var RRDStream = module.exports = function(options) {
  if (!(this instanceof RRDStream))
    return new RRDStream(options);

  Transform.call(this);

  this.buffer = new Buffer(0);
  this.rra_index = 0;

  this.options = extend({
    // Default to last 24h
    start: Date.now()/1000 - 86400,
    end: Date.now()/1000,
  }, options);
}

util.inherits(RRDStream, Transform);

RRDStream.prototype._transform = function(chunk, encoding, done) {
  this.buffer = Buffer.concat([this.buffer, chunk]);

  if (!this.header && this.buffer.length >= 128) {
    try {
      this.header = new RRDHeader(this.buffer.slice(0, 128));
    } catch (e) {
      this.emit('error', e);
    }
  }

  if (this.header && !this.header.ds && this.buffer.length >= this.header.header_size) {
    this.header.parse(this.buffer.slice(0, this.header.header_size));
    this.buffer = this.buffer.slice(this.header.header_size);
    this.emit('header', this.header);

    // Find the RRA that has the best coverage for the requested period.
    this.best_rra = {index:this.header.rra_cnt - 1, coverage:0};

    this.header.rra.forEach((function(rra, index) {
      var rra_end = this.header.last_update;
      var rra_start  = rra_end - (rra.rows * rra.pdp_per_row * this.header.step);

      // Out of bound
      if (this.options.end < rra_start || this.options.start > rra_end)
        return;

      rra_start = (this.options.start > rra_start) ? this.options.start : rra_start;
      rra_end = (this.options.end < rra_end) ? this.options.end : rra_end;

      var coverage = rra_end - rra_start;

      if (coverage > this.best_rra.coverage)
        this.best_rra = {index:index, coverage:coverage};
    }).bind(this));
  }

  if (!this.header || !this.header.ds)
    return done();

  // Parse each row
  while (this.buffer && this.buffer.length >= 8 * this.header.ds_cnt) {
    console.assert(this.rra_index <= this.best_rra.index);

    if (!this.rra) {
      this.rra = new RRDRRA(this.header, this.rra_index);

      if (this.rra_index === this.best_rra.index) {
        this.emit('rra', this.header.rra[this.rra_index]);

        this.rra.on('row', (function(timestamp, values) {
          console.assert(timestamp <= this.header.last_update);
          if (timestamp >= this.options.start && timestamp <= this.options.end) {
            this.emit('row', timestamp, values);
            this.push('{"'+timestamp+'":'+JSON.stringify(values)+'}\n');
          }
        }).bind(this));

        this.rra.on('end', (function() {
          // Ignore remaining buffer
          this.buffer = new Buffer(0);
          this.end();
        }).bind(this));
      }

      this.rra.on('end', (function() {
        this.rra_index++;
        this.rra = null;
      }).bind(this));
    }

    this.rra.push(this.buffer.slice(0, 8 * this.header.ds_cnt));
    this.buffer = this.buffer.slice(8 * this.header.ds_cnt);
  }

  done();
}

function RRDHeader(data) {
  console.assert(data.length === 128);

  if (data.slice(0,3).toString() !== RRD_COOKIE)
    throw new Error('Not an RRD file');

  this.version = data.slice(4,8).toString();
  if (this.version !== RRD_VERSION)
    throw new Error('Unsupported RRD version');

  if (data.readDoubleLE(16) !== FLOAT_COOKIE)
    throw new Error('Unsupported platform');

  this.ds_cnt = data.readUInt32LE(24);
  this.rra_cnt = data.readUInt32LE(32);
  this.step = data.readUInt32LE(40);

  this.header_size = 128 +                              // Static header
                     120 * this.ds_cnt +                // Data sources definitions
                     120 * this.rra_cnt +               // RRA definitions
                     16 +                               // Live header
                     112 * this.ds_cnt +                // PDP prep
                     80 * this.rra_cnt * this.ds_cnt +  // CDP prep
                     8 * this.rra_cnt;                  // RRA row pointer
}

RRDHeader.prototype.parse = function(data) {
  console.assert(data.length === this.header_size);

  var offset = 128;

  // Data sources definitions
  this.ds = [];
  for (var i = 0; i < this.ds_cnt; i++, offset += 120) {
    var name = data.toString('ascii', offset, offset + 20).split('\x00')[0];
    var type = data.toString('ascii', offset + 20, offset + 40).split('\x00')[0];
    var heartbeat = data.readUInt32LE(offset + 40);
    var min = data.readDoubleLE(offset + 48);
    var max = data.readDoubleLE(offset + 56);
    this.ds.push({name:name, type:type, heartbeat:heartbeat, min:min, max:max});
  }

  // RRA definitions
  this.rra = [];
  for (i = 0; i < this.rra_cnt; i++, offset += 120) {
    var cf = data.toString('ascii', offset, offset + 20).split('\x00')[0];
    var rows = data.readUInt32LE(offset + 24);
    var pdp_per_row = data.readUInt32LE(offset + 32);
    var xff = data.readDoubleLE(offset + 40);
    this.rra.push({index:i, cf:cf, rows:rows, pdp_per_row:pdp_per_row, xff:xff});
  }

  // Live header
  this.last_update = data.readUInt32LE(offset);
  offset += 16;

  // PDP prep
  for (i = 0; i < this.ds_cnt; i++, offset += 112) {
    this.ds[i].last_ds = data.toString('ascii', offset, offset + 30).split('\x00')[0];
    this.ds[i].unknown_sec = data.readUInt32LE(offset + 32);
    this.ds[i].value = data.readDoubleLE(offset + 40);
  }

  // CDP prep
  for (i = 0; i < this.rra_cnt; i++) {
    this.rra[i].cdp_prep = [];
    for (var j = 0; j < this.ds_cnt; j++, offset += 80) {
      var value = data.readDoubleLE(offset);
      var unknown_datapoints = data.readUInt32LE(offset + 8);
      this.rra[i].cdp_prep.push({value:value, unknown_datapoints:unknown_datapoints});
    }
  }

  // RRA row pointer
  for (i = 0; i < this.rra_cnt; i++, offset += 8) {
    this.rra[i].cur_row = data.readUInt32LE(offset);
  }

  console.assert(this.header_size === offset);
}

function RRDRRA(header, rra_index) {
  console.assert(rra_index < header.rra_cnt);

  extend(this, header.rra[rra_index]);

  this.row_count = 0; // Number of rows processed so far
  this.buffered_rows = [];
  this.step = this.pdp_per_row * header.step;
  this.end = parseInt(header.last_update / this.step) * this.step;
  this.start = this.timestamp = this.end - ((this.rows - 1) * this.step);
  this.ds = header.ds;
}

util.inherits(RRDRRA, EventEmitter);

RRDRRA.prototype.push = function(row) {
  console.assert(row.length === this.cdp_prep.length * 8);
  console.assert(this.row_count < this.rows);

  var values = {};
  for (var i = 0; i < this.cdp_prep.length; i++)
    values[this.ds[i].name] = row.readDoubleLE(i * 8);

  if (this.row_count > this.cur_row) {
    // Immediately emit all rows after cur_row
    this.emit('row', this.timestamp, values);
    this.timestamp += this.step;
  } else {
    // Buffer all rows up to and including cur_row
    this.buffered_rows.push(values);
  }

  this.row_count++;

  // Reached end of RRA
  if (this.row_count === this.rows) {
    // Flush buffered rows
    this.buffered_rows.forEach((function(values) {
      this.emit('row', this.timestamp, values);
      this.timestamp += this.step;
    }).bind(this));

    this.emit('end');
  }
}
