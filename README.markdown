mongueue is a simple and nasty mongodb-based queue.
it uses mongolian collections as the queue storage.

usage:

```javascript

var
  mongodb = require('mongodb'),
  Mongueue = require('../mongueue');

var q,
    options = {
    user: "user",
    pass: "pass",
    dbname: "cron",
	queuename: "name",
    host: "localhost",
    port: 6000,
    collection: 'queueName'
};

q = new Mongueue(null, options, function (err, mongueue) {
        if (err) {
            test.ok(!err, "failed to create queue" + err.errmsg);
            test.done();
        }
        sample();
    })

        
function sample() {
  q.waitDequeue(
  10, // ttl (in seconds)
  2,  // backoff (in seconds)
  function(err, item, releasefn) {
    console.log("the following item was dequeued:", item);
    releasefn(err);
    q.stop();
  });
    
  q.enqueue("this is the item to enqueue. any javascript object is good", function(err) {
    if (err) console.error("couldn't queue the item");
    else console.log("item queued");
  });
}

```