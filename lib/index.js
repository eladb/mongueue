var bson = require('mongodb').BSONPure,
    assert = require('assert'),
    Mongolian = require('mongolian');

//
// constructor
// collection: a mongolian collcetion
var Mongueue = module.exports = function(collection) {
  this.collection = collection;
};

//
// enqueues an item to the queue.
// item: any mongodb capable object
// callback: function(error, [queue])
Mongueue.prototype.enqueue = function(item, callback) {
  var entry = {
    createdAt: new bson.Timestamp(),
    hiddenUntil: null,
    payload: item
  };
  
  var q = this;
  this.collection.save(entry, function(err) {
    if (callback) callback(err, q);
  });
};

//
// dequeues an item
// ttl: number of seconds to allow before the item will pop back into the
//      beginning of the queue if release_callback is not called.
// callback: function(err, item, release_callback)
//           you must call the release_callback in order to release the
//           item from the queue.
Mongueue.prototype.dequeue = function(ttl, callback) {
  if (typeof ttl != "number" || ttl <= 0) throw "ttl must be a number in seconds > 0";
  if (!callback || typeof callback != "function") throw "callback must be a function";
  
  var now = new bson.Timestamp(new Date().getTime(), 0);
  var hiddenUntil = new bson.Timestamp(new Date(new Date().getTime() + ttl * 1000), 0);
  var coll = this.collection; 

  this.collection.findAndModify({
    query  : { $or: [ { hiddenUntil: null }, { hiddenUntil: { $lt: now } } ] },
    update : { $set: { hiddenUntil: hiddenUntil } },
    sort   : { "createdAt" : 1 },
    "new"  : true // means to return the new item 
  }, function(err, item) {
    
    //
    // a function passed as callback and called if we want to avoid release 
    // the object but still invoke the callback if given.
    function dontReleaseFn(err, cb) {
      if (cb) cb();
    };

    //
    // a function passed as callback and releases the object unless
    // there was an error
    function releaseFn(err, cb) {
      // if we are called with an error, don't release
      if (err) {
        if (cb) cb();
        return;
      } 
            
      // otherwise, release
      coll.remove({ _id: item._id }, function(err) {
        if (cb) cb();
      });
    };
    
    // if we we had an error from find & modify.
    if (err) {
      if (err.result && err.result.errmsg == 'No matching object found') {
        // if we couldn't find the item, invoke the callback
        // with a null indicating there are no items
        // to be dequeued. 
        callback(null, null, dontReleaseFn);
        return;
      }

      // other error occured, invoke the callback with
      // an explicit error.
      callback(err, null, dontReleaseFn);
      return;
    }
    
    // ok, we found the item, pass it back via the callback
    // alongwith a release method.
    callback(null, item.payload, releaseFn);
  });
};

//
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

//
// counts how many items are in the queue (incl. hidden items).
// callback: function(err, count)
Mongueue.prototype.count = function(callback) {
  this.collection.count(callback);
};

//
// counts how many hidden items are in the queue (items that are currently being processed).
// callback: function(err, count)
Mongueue.prototype.hiddenCount = function(callback) {
  var coll = this.collection;

  // map: all entities that are hidden into 'hiddenCount' with the value 1.
  function map() {
    if (this.hiddenUntil && this.hiddenUntil > new Timestamp()) {
      emit("hiddenCount", 1); 
    }
  }
  
  // reduce: just count how many items are in a key
  function reduce(key, arr) {
    return arr.length;
  }
  
  // invoke m/r
  coll.mapReduce(map, reduce, { out: { inline:1 } }, function(err, result) {
    if (err) {
      if (err.result && err.result.errmsg == 'ns doesn\'t exist') {
        callback(null, 0);
        return;
      }
      
      callback(err);
      return;
    }
    
    var value = result.results.length > 0 ? result.results[0].value : 0; 
    callback(null, value);
  });
};
