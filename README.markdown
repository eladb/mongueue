mongueue is a simple and nasty mongodb-based queue.
it uses mongolian collections as the queue storage.

usage:

```javascript

var
  Mongolian = require('mongolian'),
  Mongueue = require('../mongueue');

var db = new Mongolian("mongo://localhost:60000/db");
var q = new Mongueue(db.collection('hellogueue'));

q.waitDequeue(
  10, /* ttl (in seconds) */
  2,  /* backoff (in seconds) */
  function(err, item, releasefn) {
    console.log("the following item was dequeued:", item);
    releasefn(err);
  });

q.enqueue("this is the item to enqueue. any javascript object is good", function(err) {
  if (err) console.error("couldn't queue the item");
  else console.log("item queued");
});
```