var Mongueue = require('mongueue'),
    testCase = require('nodeunit').testCase;

exports.MongueueRescheduleTests = testCase({
    ScheduleNull: testReschedule({reoccurrence: null, now: new Date(), expected: 0}),
    ScheduleNoDaily: testReschedule({reoccurrence: [], now: new Date(), expected: 0}),
    NoSchedule: testReschedule({reoccurrence: {daily: [[], [], [], [], [], [], []]}, now: new Date(), expected: 0}),
    TodayLaterNotSorted: testReschedule({reoccurrence: {daily: [[46805, 46800 /*13:00 sunday*/], [], [], [], [], [], []]}, now: new Date(1322389773823), expected: 1827}),
    TodayLaterSorted: testReschedule({reoccurrence: {daily: [[46800, 46805 /*13:00 sunday*/], [], [], [], [], [], []]}, now: new Date(1322389773823), expected: 1827}),
    NextWeekSameDaySorted: testReschedule({reoccurrence: {daily: [[0, 10 /*00:00 sunday*/], [], [], [], [], [], []]}, now: new Date(1322389773823), expected: 559827}),
    NextWeekSameDayNotSorted: testReschedule({reoccurrence: {daily: [[10, 0 /*00:00 sunday*/], [], [], [], [], [], []]}, now: new Date(1322389773823), expected: 559827}),
    TomorrowSorted:  testReschedule({reoccurrence: {daily: [[], [44973, 50000], [], [], [], [], []]}, now: new Date(1322389773823), expected: 86400}),
    TomorrowNotSorted:  testReschedule({reoccurrence: {daily: [[], [50000, 44973], [], [], [], [], []]}, now: new Date(1322389773823), expected: 86400}),
    NextWeekSundayNotSorted:  testReschedule({reoccurrence: {daily: [[50000, 44973], [], [], [], [], [], []]}, now: new Date(1322476173823), expected: 518400}),
    NextWeekSundaySorted:  testReschedule({reoccurrence: {daily: [[50000, 44973], [], [], [], [], [], []]}, now: new Date(1322476173823), expected: 518400}),
    TwiceOnTheSameTime: testReschedule({reoccurrence: {daily: [[44973, 44973, 45000], [], [], [], [], [], []]}, now: new Date(1322389773823), expected: 27}),
    TwiceOnTheSameTimeNextWeek: testReschedule({reoccurrence: {daily: [[44973, 44973], [], [], [], [], [], []]}, now: new Date(1322389773823), expected: 604800})
});

function testReschedule(scenario) {
    return function (test) {
        var q = new Mongueue({}),
            nextInSec = q.next(scenario.reoccurrence, scenario.now);
        test.ok(scenario.expected == nextInSec, "Expected next time " + nextInSec + '==' + scenario.expected);
        test.done();
    }
}