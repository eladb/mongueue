var Mongueue = require('mongueue'),
    mongodb = require('mongodb'),
    testCase = require('nodeunit').testCase;


var queueName = 'TestMongueue';

exports.mongueueCountTests = testCase({
    'mongueue count test0': createTest({count: 0, hiddenCount: 0, name: 'mongueue count test0'}, countTest),
    'mongueue count test1': createTest({count: 1, hiddenCount: 1, name: 'mongueue count test1'}, countTest),
    'mongueue count test2': createTest({count: 2, hiddenCount: 2, name: 'mongueue count test2'}, countTest)
});


function createTest (scenario, testMethod) {
    return function(test) {
        test.testScenario = scenario;
        setUpTests(test, testMethod);
    };
}

function countTest(test) {
    var i, inserted = 0;
    test.testName = test.testScenario.name;

    function testVerify (err, itemid) {
        if (++inserted == test.testScenario.count || test.testScenario.count == 0) {
            if (err) {
                console.error("couldn't queue the item");
                items[itemid] = 'failure';
                test.ok(!err, 'got error on insert');
                return done(test);
            }

            test.q.count(function(err, count) {
                if (err) {
                    console.error("failed to get items count", err);
                    test.ok(!err, 'got error on count');
                    return done(test);
                }
                if (count !== test.testScenario.count) {
                    test.ok(count === test.testScenario.count, count + '(queuecount)===(scenariocount)' + test.testScenario.count);
                    return done(test);
                }

                console.log(test.testName + ': finished count moving to hidden count');
                return test.q.hiddenCount(function (err, hiddenCount) {
                    if (err) {
                        console.error("failed to get items hidden count", err);
                        test.ok(!err, 'got error on hidden count');
                        return done(test);
                    }

                    if (hiddenCount !== test.testScenario.hiddenCount) {
                        test.ok(hiddenCount === test.testScenario.hiddenCount, hiddenCount + '(queue hidden count)===(scenario hidden count)' + test.testScenario.hiddenCount);
                        return done(test);
                    }
                    return done(test);
                });
            });
        }
    }

    for(i=0; i<test.testScenario.count; i++) {
        test.q.enqueue([{'url': "http://www.bing.com/", 'method': 'GET', 'json': false}],
            {insertat: new Date(new Date().getTime() + 3600 * 1000)},
            testVerify);
    }

    if (test.testScenario.count === 0) {
        console.log('empty test verify count and hidden count are both zero');
        testVerify(undefined, undefined);
    }
}

function rand() {
    return Math.round(Math.random() * 10000).toString();
}

function setUpTests (test, callback) {
// mongolab.com options
    var options = {
        user: "ork",
        pass: "orkie1234",
        dbname: "cron",
        queuename: "test" + rand(),
        host: "ds029107.mongolab.com",
        port: 29107
    };

    test.q = new Mongueue(null, options, function (err, mongueue) {
        if (err) {
            test.ok(!err, "failed to create queue" + err.errmsg);
            test.done();
        }
        callback(test);
    })
}

function done(test) {
    setTimeout(function() {
        console.log("finishing test", test.testName);
        test.ok(true, test.testName)
        test.done();
        test.q.stop(true);
    }, 1000);
}