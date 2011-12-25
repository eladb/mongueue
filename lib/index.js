var assert = require('assert'),
    mongodb = require('mongodb'),
    bson = require('mongodb').BSONPure;

//
// constructor
// collection: a mongolian collcetion
// can create the mongueue collect async, using mongo account settings {host, port, dbname, queuename, user, pass}
var Mongueue = module.exports = function(collection, account, callback) {
    if (!(collection ||  (account && callback))) throw new Error('mongo collection or account is required');

    this.timers = [];
    if (collection) {
        this.collection = collection;
        this.started = true;
    }
    else {
        this.started = false;
        setupCollection(this, account, callback);
    }
};


// setup a new collection for mongueue
function setupCollection(mongueue, options, callback) {
    if (!options.host) callback({status: -1, errmsg: 'host is not defined'});
    if (!options.port) callback({status: -1, errmsg: 'port is not defined'});
    if (!options.dbname) callback({status: -1, errmsg: 'db name is not defined'});
    if (!options.queuename) callback({status: -1, errmsg: 'queue name is not defined'});
    var server = new mongodb.Server(options.host, options.port, { auto_reconnect: true });

    new mongodb.Db(options.dbname, server, {}).open(function (err, client) {
        if (err) callback(err);
        mongueue.mongoClient = client;
        if (typeof options.user !== 'string' || typeof options.pass !== 'string') return initMongueue(null, mongueue, options, callback);
        mongueue.mongoClient.authenticate(options.user, options.pass, function (err) {initMongueue(err, mongueue, options, callback)});
    });
}

// after authenticating, set the collection for the mongueue
function initMongueue(err, mongueue, options, callback) {
    if (err) {
        console.error('Got error on connect', err);
        callback({status: -1, errmsg: 'failed to connect'});
    }

    mongueue.collection = new mongodb.Collection(mongueue.mongoClient, options.queuename);
    mongueue.start();
    callback(null, mongueue);
}

//
// enqueues an item to the queue.
// item: any mongodb capable object
// schedule: schedule of the message dequeue - contract {daily: [[324],[],[],[],[],[],[]], insertat: date, single: true/false}"
//     - daily - a list of seconds on day, in which the message should be dequeued
//     - insertat - a date in which the message will first dequeue (delayed message)
//     - single - state whether to dequeue the message only once
// callback: function(error, [queue])
Mongueue.prototype.enqueue = function(item, schedule, callback) {

    var hiddenDate;
    if (!callback) callback = function() {};
    if (schedule && schedule.insertat) {
        hiddenDate = new Date(schedule.insertat);
    }
    else {
        var now = new Date();
        var nextTime = this.next(schedule, now) * 1000;
        var hiddenDate = new Date(now.getTime() + nextTime);
        console.log("next : ", nextTime);
    }
      
    var entry = {
        createdAt: new Date(),
        hiddenUntil: hiddenDate,
        payload: item,
        reoccurrence: schedule
    };
      
    this.collection.save(entry, function(err) {
        callback(err, entry['_id']);
    });
};

// 
// get the next occurrence of an item in seconds from now
// reoccurrence:
// now: a date object which represents the time of the next query
// reoccurrence: only daily is currently supported, send an object such as:
//     {daily: [[],[],[],[],[],[],[]]}
Mongueue.prototype.next = function(reoccurrence, now) {
    if (!now) now = new Date();
    if (!reoccurrence || !reoccurrence.daily) return 0;
    
    var secondsToday = now.getSeconds() + (now.getMinutes() + now.getHours() * 60) * 60;
    var today = now.getDay();
    var eventsToday = reoccurrence.daily[today].sort();
    var diff = 0;

    // get next event today if exists
    var len = eventsToday.length;
    for (var i = 0; i < len; i++)
    {
        var event = eventsToday[i];
        console.log("today : ", secondsToday , "event : ", event);
        if (event > secondsToday)
        {
            diff = event - secondsToday;
            break;
        }
    }

    // get next event on the rest of the week days if exists
    if (diff === 0)
    {
      for (var i = 1; i < 7; i++)
      {
        if (reoccurrence.daily[(today + i) % 7].length != 0)
        {
            console.log("number of days diff : ", i);
            diff = (i * 24 * 60 * 60) + reoccurrence.daily[(today + i) % 7].sort()[0] - secondsToday;
            break;
        }
      }
    }

    // get next event on the same week day as today on next week
    if (diff === 0)
    {
        if (reoccurrence.daily[today].length != 0)
        {
            // set to next week
            diff = reoccurrence.daily[today].sort()[0] - secondsToday + 7 * 24 * 60 * 60;
            console.log("setting revival to next week : ", diff);
        }
    }
    
    return diff;
}

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
  
  var self = this;
  var now = new Date();
  var hiddenUntil = new Date((new Date()).getTime() + ttl * 1000);
  var coll = this.collection;

  this.collection.findAndModify(
    { $or: [ { hiddenUntil: null }, { hiddenUntil: { $lt: now } } ] },  // query
      [[ 'createdAt', 'asc']],                                          // sort by old to new
      { $set: { hiddenUntil: hiddenUntil } },                           // update
      {"new"  : true}                                                   // return the new item
   , function(err, item) {
    
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
      coll.remove({ _id: item._id }, {}, function(err) {
        if (cb) cb();
      });
    };
    
    //
    // a function passed as callback and requeues the object even if
    // there was an error
    function requeueFn(err, cb) {
      var diff = self.next(item.reoccurrence, now);
      
      if (diff === 0)
      {
        releaseFn(null, cb);
        return;
      }
      
      var revival = new Date(now.getTime() + (diff * 1000));
      
      console.log("setting revival to: ", revival.toString());
      
      // set the revival date
      coll.findAndModify(
        { _id: item._id },                  // query
    [],                                     // no sort (query single item by id)
        { $set: { hiddenUntil: revival } }, // set to revival
        {"new"  : true}                     // return the old item (not using it)
      , function(err, item) {
        if (cb) cb(err);
      });
    };
    
    // if we we had an error from find & modify.
    if (err) {
      if (err.ok === 0 && err.errmsg == 'No matching object found') {
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


    var isSingle = item.reoccurrence && item.reoccurrence.single;
    // Delete disposable items from reoccurrence to make it extensible
    if (item.reoccurrence) {
        delete item.reoccurrence['single'];
        delete item.reoccurrence['insertat'];
    }

    if (!item)
    {
        // null item, call callback with null
        return callback(null, null, releaseFn, null);
    }
    else if (!item.reoccurrence || !item.reoccurrence.length == 0 || isSingle) {
        // ok, we found the item, pass it back via the callback
        // alongwith a release method.
        return callback(null, item.payload, releaseFn, item._id);
    }
    else {
        // ok, we found the item with reoccurrence, pass it back via the callback
        // along with a requeue method.
        return callback(null, item.payload, requeueFn, item._id);
    }
  });
};

//
// tries to dequeue an item. if there are no items, waits 'backoff' seconds
// and tries again. this is done until an item is available.
Mongueue.prototype.waitDequeue = function (ttl, backoff, callback) {
    var self = this,
        timer;
    (function dequeueAndWait() {
        var timerIndex;

        // delete the current timer if exists, used for stop
        if (timer) {
            timerIndex = self.timers.indexOf(timer);
            if (!!~timerIndex) {
                self.timers.splice(timerIndex, 1);
            }
        }

        if (!self.started) {
            console.log('Mongugue - we are stopped, ignore dequeue event');
            return;
        }

        self.dequeue(ttl, function (err, item, releasecb, context) {
            // if there was an error, pass it along.
            if (err) {
                callback(err, item, releasecb);
                return;
            }

            // if there was no item, wait 'backoff' seconds,
            // and recursively call dequeueAndWait.
            if (!item) {
                timer = setTimeout(dequeueAndWait, backoff * 1000);
                // adding the timer to the timers list in case we get stop request
                self.timers.push(timer);
                return;
            }


            // call the dequeue callback with the dequeued item.
            callback(err, item, releasecb, context);
        });
    })();
};

//
// restarting the queue
//
Mongueue.prototype.start = function() {
    this.started = true;
};

//
// stopping the queue
//
Mongueue.prototype.stop = function(drop) {
    var self = this;
    this.started = false;
    console.log('Mongugue - stopping dequeue', this.timers.length);

    this.timers.forEach(function (timer) {
        console.log('Mongugue - stopping fetcher');
        clearTimeout(timer);
    });

    this.timers = [];

    if(drop) {
        return this.collection.drop(
            function () {
                if (self.mongoClient) {
                    self.mongoClient.close();
                }
        });
    }
    else if (this.mongoClient) this.mongoClient.close();
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
    if (this.hiddenUntil && this.hiddenUntil > new Date()) {
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
      if (err.errmsg && err.errmsg == "ns doesn't exist") {
        callback(null, 0);
        return;
      }
      
      callback(err);
      return;
    }

    var value = result.length > 0 ? result[0].value : 0;
    callback(null, value);
  });
};

//
// removes an item from the queue by its id
// useful for removing scheduled tasks
// itemId: id of item to remove
// callback: function(err)
Mongueue.prototype.remove = function(itemId, callback) {
    var objid = new bson.ObjectID(itemId);
    this.collection.remove(
        {_id: objid},
        {},
        function(err) {
            if (callback) callback(err);
        });
};

//
// query for an item, by its id
// itemId: id of item to remove
// callback: function(err, queriedItem)
Mongueue.prototype.query = function (itemId, callback) {
    if (!callback) return;

    var objid = new bson.ObjectID(itemId);

    this.collection.findOne({ '_id': objid }, {}, function (err, item) {
        if (err) {
            callback(err)
        } else {
            callback(null, item);
        }
    });
};
