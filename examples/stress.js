var
  async = require('async'),
  assert = require('assert'),
  mongodb = require('mongodb'),
  Mongueue = require('../mongueue');


var options = {
  user: "user",
  pass: "pass",
  name: "dbname",
  host: "localhost",
  port: 6000,
  collection: 'queueName'
};

var queue,
  stopped = false,
  server = new mongodb.Server(options.host, options.port, { auto_reconnect: true }),
  mongoClient,
  mongoCollection;


new mongodb.Db(options.name, server, {}).open(function (err, client) {
    mongoClient = client;
    if (typeof options.user !== 'string' || typeof options.pass !== 'string') return InitQueue(err, sample);
    mongoClient.authenticate(options.user, options.pass, function (err) {InitQueue(err, sample)});
});


function InitQueue(err, callback) {
    var self = this;
    if (err) {
        console.log('Got error on connect', err);
        return;
    }

    mongoCollection = new mongodb.Collection(mongoClient, options.collection);

    queue = new Mongueue(mongoCollection);
    if (callback) callback();
}


function sample() {
  function processQueue(q) {
      if (stopped) return;
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
	
  // stop after 5 minutes seconds
  setTimeout(function () {
	console.log('stopping example');
	queue.stop();
	stopped = true;
	// close the client after 10 seconds
	setTimeout(function() {
	  console.log('removing all items and stopping the client');
	  mongoCollection.remove({}, function() {
		console.log('closing the client');
		mongoClient.close();
		throw('finished');
	  });
	}, 10*1000);
  }, 5 * 60 * 1000);
}