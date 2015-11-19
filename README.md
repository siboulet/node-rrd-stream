[![Build Status](https://travis-ci.org/siboulet/node-rrd-stream.svg?branch=master)](https://travis-ci.org/siboulet/node-rrd-stream)

# node-rrd-stream
**Node.js library for streaming RRD file**

This library streams newline-delimited JSON from an RRD file (round-robin database) and is compatible with RRDtool. It's written in pure JavaScript and doesn't have any external dependency.

# Example

```js
var fs = require('fs');
var rrdStream = require('rrd-stream');

// Last 90 days
var end = Date.now() / 1000;
var start = end - 60*60*24*90;

var rrd = fs.createReadStream('sample.rrd');
rrd.pipe(rrdStream({start:start,end:end})).pipe(process.stdout);
```

# Events

## header

This event fires with the header of the RRD file.

## rra

This events fires with the RRA that best matched the requested period.

## row

This events fires with timestamp and values of each row in the requested period. It can be used to customize the output if newline-delimited JSON is not what you want.

```js
rrd.pipe(rrdStream({start:start,end:end})
  .on('row', function(timestamp, values) {
    console.log(timestamp, values);
  })
);
```
## error

This events fire when the RRD file is invalid.
