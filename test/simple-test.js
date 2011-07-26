var vows = require('vows'),
    assert = require('assert'),
    Mongueue = require('../mongueue.js'),
    Mongolian = require('mongolian'),
    async = require('async');

var db = new Mongolian('mongo://localhost:60000/mongueue_test');

//
// some vows 'macros'

function assertCount(number) {
  var ctx = {};
  ctx["topic"] = function(q) { q.count(this.callback); };
  ctx["is " + number.toString()] = function(cnt) { assert.equal(cnt, number); };  
  return ctx;
}

function assertHiddenCount(number) {
  var ctx = {};
  ctx["topic"] = function(q) { q.hiddenCount(this.callback); };
  ctx["is " + number.toString()] = function(cnt) { assert.equal(cnt, number); };  
  return ctx;
}

function dequeue(q, count, shouldRelease) {
  var functions = [];
  
  for (var i = 0; i < count; ++i) {
    functions.push(function(cb) {
      q.dequeue(10, function(err, item, rcb) {
        if (shouldRelease) {
          rcb(err, function(err2) {
            cb(err2, q);
          });
        } else {
          cb(err, q);
        }
      });
    });
  }

  return functions;
}

//
// the test suite

vows.describe('mongueue tests').addBatch({
  "with a clean database": {
    topic: function() {
      var cb = this.callback;
      db.dropDatabase(function(err) {
        if (err) cb(err);
        else cb(null, db);
      });
    },
    "testqueue": {
      topic: function(db) { return new Mongueue(db.collection('testqueue')); },
      "is not null": function (q) { assert.isObject(q); },
      "with an enqueued item": {
        topic: function(q) {
          var cb = this.callback; 
          q.enqueue("my item", function(err, item) {
            cb(err, q);
          });
        },
        "count": assertCount(1),
        "hiddenCount": assertHiddenCount(0),
        "then dequeue.": {
          topic: function(q) { q.dequeue(60, this.callback); },
          "the payload should be 'my item'": function(item) { assert.equal(item, "my item"); },
          "before releasing,": {
            topic: function(item, releasecb, q) { return q; },
            "if we dequeue again": {
              topic: function(q) { q.dequeue(10, this.callback); },
              "we get null (empty queue)": function(item) { assert.isNull(item); }
            },
            "count": assertCount(1),
            "hiddenCount": assertHiddenCount(1),
          }
        }
      }
    },
    "testqueue2": {
      topic: function(db) { return new Mongueue(db.collection('testqueue2')); },
      "is empty": assertCount(0),
      "and no hidden items": assertHiddenCount(0),
      "enqueue 10 items": {
        topic: function(q) {
          var topiccb = this.callback;
          var enqueues = [];
          for (var i = 0; i < 10; ++i) {
            enqueues.push(function(cb) {
              q.enqueue("item" + (Math.random() * 1000).toString(), cb);
            });
          }
          
          async.parallel(enqueues, function() { topiccb(null, q); });
        },
        "we have 10 items": assertCount(10),
        "none hidden": assertHiddenCount(0),
        "dequeue 5": {
          topic: function(q) {
            var topiccb = this.callback;
            async.parallel(
              dequeue(q, 3, false).concat(dequeue(q, 5, true)),
              function(err) { topiccb(err, q); });
          },
          "count is 5 (because we didn't release)": assertCount(5),
          "hidden count is 3": assertHiddenCount(3),
          "we can now dequeue 2 more": {
            topic: function(q) {
              var topiccb = this.callback;
              async.parallel(
                dequeue(q, 2, true),
                function(err) { topiccb(err, q); });
            },
            "we should have 3 items": assertCount(3),
            "still 3 hidden": assertHiddenCount(3),
            "if we try to dequeue again": {
              topic: function(q) {
                var topiccb = this.callback;
                q.dequeue(10, function(err, item, rcb) {
                  topiccb(err, item);
                });
              },
              "returns null": function(item) {
                assert.isNull(item);
              }
            }
          }
        }
      }
    },
    "testqueue3": {
      topic: function(db) { return new Mongueue(db.collection('testqueue3')); },
      "enqueue an item": {
        topic: function(q) {
          q.enqueue("my item", this.callback);
        },
        "count should be 1": assertCount(1),
        "hidden should be 0": assertHiddenCount(0),
        "and now dequeue": {
          topic: function(q) {
            var cb = this.callback;
            q.dequeue(1, function(err, item, rcb) {
              cb(err, q);
            });
          },
          "wait 1 second": {
            topic: function(q) {
              var cb = this.callback;
              setTimeout(function() { cb(null, q); }, 1 * 1000);
            },
            "and expect that the item will now be available for dequeue again": {
              topic: function(q) {
                var cb = this.callback;
                q.dequeue(1, function(err, item, rcb) {
                  rcb(err, function(err2) {
                    cb(err2, item);
                  });
                });
              },
              "item is right": function(item) {
                assert.equal(item, "my item");
              }
            }
          }
        }
      }
      
    }
  }
}).export(module);