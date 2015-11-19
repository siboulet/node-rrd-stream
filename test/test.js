var assert = require('assert');
var fs = require('fs');
var rrdStream = require('../');

/*
rrdtool create test/test.rrd \
   --start 20150101 --step 1 \
   DS:test:GAUGE:300:0:10 \
   RRA:LAST:0.5:1:10 \
   RRA:AVERAGE:0.5:10:6 \
   RRA:AVERAGE:0.5:60:5
*/

describe('node-rrd-stream', function() {

  it('should emit header', function(done) {
    fs.createReadStream('test/test.rrd')
      .pipe(rrdStream())
      .once('header', function(header) {
        assert.equal(header.constructor.name, 'RRDHeader');
        assert.equal(header.rra_cnt, 3);
        assert.equal(header.rra[0].rows, 10);
        assert.equal(header.rra[1].rows, 6);
        assert.equal(header.rra[2].rows, 5);
        assert.equal(header.ds[0].name, 'test');
        done();
      });
  });

  it('should emit row for matching rra', function(done) {
    var rowCount = 0;
    fs.createReadStream('test/test.rrd')
      .pipe(rrdStream({start:1420070391,end:1420070400}))
      .once('rra', function(rra) {
        assert.equal(rra.index, 0);
        assert.equal(rra.cf, 'LAST');
      })
      .once('row', function(timestamp, values) {
        assert.equal(timestamp, 1420070391);
      })
      .on('row', function(timestamp, values) {
        assert(values.hasOwnProperty('test'));
        rowCount++;
      })
      .on('unpipe', function() {
        assert.equal(rowCount, 10);
        done();
      });
  });

});
