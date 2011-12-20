mongueue is a simple and nasty mongodb-based queue.
it uses mongolian collections as the queue storage.

usage:

```javascript

var
  mongodb = require('mongodb'),
  Mongueue = require('../mongueue');

var q;
var mongoClient;
var options = {
    user: "user",
    pass: "pass",
    name: "dbname",
    host: "localhost",
    port: 6000,
    collection: 'queueName'
};

var server = new mongodb.Server(options.host, options.port, { auto_reconnect: true });

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

    var mongoCollection = new mongodb.Collection(mongoClient, options.collection);
    
    q = new Mongueue(mongoCollection);
    if (callback) callback();
}
        
function sample() {
  q.waitDequeue(
  10, // ttl (in seconds)
  2,  // backoff (in seconds)
  function(err, item, releasefn) {
    console.log("the following item was dequeued:", item);
    releasefn(err);
    mongoClient.close();
  });
    
  q.enqueue("this is the item to enqueue. any javascript object is good", function(err) {
    if (err) console.error("couldn't queue the item");
    else console.log("item queued");
  });
}
```