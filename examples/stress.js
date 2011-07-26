var
  async = require('async'),
  assert = require('assert'),
  Mongolian = require('mongolian'),
  Mongueue = require('../mongueue');

var db = new Mongolian('mongo://localhost:60000/mongueue_stress_test');
var queue = new Mongueue(db.collection('stressqueue'));

// tries to dequeue an item. if there are no items, waits 'backoff' seconds
// and tries again. this is done until an item is available.
Mongueue.prototype.waitDequeue = function(ttl, backoff, callback) {
  var self = this;
  (function dequeueAndWait() {
    self.dequeue(ttl, function(err, item, releasecb) {
      // if there was an error, pass it along.
      if (err) {
        callback(err, item, releasecb);
        return;
      }
      
      // if there was no item, wait 'backoff' seconds,
      // and recursively call dequeueAndWait.
      if (!item) {
        setTimeout(dequeueAndWait, backoff * 1000);
        return;
      }
      
      // call the dequeue callback with the dequeued item.
      callback(err, item, releasecb);
    });
  })();
};

function processQueue(q) {
  q.waitDequeue(10, 5, function(err, item, rcb) {
    q.count(function(err, count) {
      q.hiddenCount(function(err, hiddenCount) {
        console.log('processing item:', item, 'count:', count, 'hidden:', hiddenCount);

        // release item.
        rcb(err, function(err) {
          if (err) console.error('unable to release item', err);
          // process next item.
          processQueue(q);
        });
      });
    });
  });
}

// start 10 concurrent process 'workers'.
for (var i = 0; i < 10; ++i) {
  processQueue(queue);
}

//var sys = require('sys');
var st = process.openStdin();
console.log('type characters and they will be enqueued');
st.addListener("data", function(data) {
  var s = data.toString();
  queue.enqueue(s, function(err) {
    console.log('enqueued: ', s);
  });
});

var actions = [];

//
// enqueue 1000 items
for (var i = 0; i < 1000; ++i) {
  actions.push(function(cb) {
    console.log('enqueued: item number ' + i.toString());
    queue.enqueue({ name: 'my item', uniqifier: Math.random() }, cb);
  });
}

async.parallel(actions, 
  function(err) { 
    console.log('enqueue/dequeue completed. now dequeue should return null');
  });