_ = require 'lodash'
async = require 'async'
RegisterJobProcessor = require '../src/register-job-processor'
redis = require 'fakeredis'
debug = require('debug')('mocha-test')
UUID = require 'uuid'

describe 'RegisterJobProcessor', ->
  beforeEach ->
    @kue = require 'kue'
    @redisKey = UUID.v1()
    @client = _.bindAll redis.createClient @redisKey

    @queue = @kue.createQueue
      jobEvents: false

    options = {
      @client
      @kue
      pingInterval: 100000
      intervalTTL: 1000
      intervalAttempts: 3
      minTimeDiff: 500
      @queue
    }

    @sut = new RegisterJobProcessor options

  describe '->processJob', ->
    context 'with intervalTime', ->
      beforeEach (done) ->
        @registerJob = @queue.create 'register', {
          sendTo: 'register-flow-id'
          nodeId: 'some-node-id'
          intervalTime: 1000
          nonce: 'this-is-nonce'
        }
        @registerJob.save done

      beforeEach (done) ->
        @sut.processJob @registerJob, {}, done

      it 'should add a pingJob', (done) ->
        @client.get 'interval/ping/register-flow-id/some-node-id', (error, jobId) =>
          return done error if error?
          @kue.Job.get jobId, (error, job) =>
            return done error if error?
            expect(job).to.exist
            done()

      it 'should add a intervalJob', (done) ->
        @client.smembers 'interval/job/register-flow-id/some-node-id', (error, jobIds) =>
          return done error if error?
          @kue.Job.get _.first(jobIds), (error, job) =>
            return done error if error?
            expect(job).to.exist
            done()

      it 'should set interval redis stuff', (done) ->
        @client.mget [
          "interval/active/register-flow-id/some-node-id"
          "interval/time/register-flow-id/some-node-id"
          "interval/cron/register-flow-id/some-node-id"
          "interval/nonce/register-flow-id/some-node-id"
        ], (error, results) =>
          return done error if error?
          expect(results).to.deep.equal ['true', '1000', '', 'this-is-nonce']
          done()

    context 'with cronString', ->
      beforeEach (done) ->
        @registerJob = @queue.create 'register', {
          sendTo: 'register-cron-flow-id'
          nodeId: 'some-node-id'
          cronString: '* * * * *'
          nonce: 'this-is-nonce'
        }
        @registerJob.save done

      beforeEach (done) ->
        @sut.processJob @registerJob, {}, done

      it 'should add a pingJob', (done) ->
        @client.get 'interval/ping/register-cron-flow-id/some-node-id', (error, jobId) =>
          return done error if error?
          @kue.Job.get jobId, (error, job) =>
            return done error if error?
            expect(job).to.exist
            done()

      it 'should add a intervalJob', (done) ->
        @client.smembers 'interval/job/register-cron-flow-id/some-node-id', (error, jobIds) =>
          return done error if error?
          @kue.Job.get _.first(jobIds), (error, job) =>
            return done error if error?
            expect(job).to.exist
            done()

  describe '->calculateNextCronInterval', ->
    describe 'using a real date with milliseconds set to 0', ->
      now = new Date
      now.setMilliseconds(0)

      describe 'when called with seconds option', ->
        it 'should result in a next time of at most 1000 ms', ->
          result = @sut.calculateNextCronInterval "* * * * * *", now
          nextDate = new Date(now.getTime() + result)
          debug 'result from', now, 'to', nextDate.toString(), result, 'ms'
          nextSecond = nextDate.getSeconds()- now.getSeconds()
          if nextSecond == -59
            nextSecond = 1
          expect(nextSecond).to.equal 1
          expect(nextDate.getMilliseconds()).to.equal 0
          expect(result).to.be.at.most(1000)

      describe 'when called with every 15 seconds option', ->
        it 'should result in a next time of at most 15000 ms', ->
          result = @sut.calculateNextCronInterval "*/15 * * * * *", now
          nextDate = new Date(now.getTime() + result)
          debug 'result from', now, 'to', nextDate.toString(), result, 'ms'
          expect(nextDate.getSeconds() % 15).to.equal 0
          expect(nextDate.getMilliseconds()).to.equal 0
          expect(result).to.be.at.most(15000)

      describe 'when called with minutes option', ->
        it 'should result in a next time of at most 60000 ms', ->
          result = @sut.calculateNextCronInterval "* * * * *", now
          nextDate = new Date(now.getTime() + result)
          debug 'result from', now, 'to', nextDate.toString(), result, 'ms'
          nextMinute = nextDate.getMinutes()- now.getMinutes()
          if nextMinute == -59
            nextMinute = 1
          expect(nextMinute).to.equal 1
          expect(nextDate.getSeconds()).to.equal 0
          expect(nextDate.getMilliseconds()).to.equal 0
          expect(result).to.be.at.most(60000)

      describe 'when called with every 15 minutes option', ->
        it 'should result in a next time of at most 900000 ms', ->
          result = @sut.calculateNextCronInterval "*/15 * * * *", now
          nextDate = new Date(now.getTime() + result)
          debug 'result from', now, 'to', nextDate.toString(), result, 'ms'
          expect(nextDate.getMinutes() % 15).to.equal 0
          expect(nextDate.getSeconds()).to.equal 0
          expect(nextDate.getMilliseconds()).to.equal 0
          expect(result).to.be.at.most(900000)

    describe 'using a real date with milliseconds set to 0', ->
      now = new Date
      now.setMilliseconds(0)

      describe 'when called with seconds option', ->
        it 'should result in a next time of at most 1000 ms', ->
          result = @sut.calculateNextCronInterval "* * * * * *", now
          nextDate = new Date(now.getTime() + result)
          debug 'result from', now, 'to', nextDate.toString(), result, 'ms'
          nextSecond = nextDate.getSeconds()- now.getSeconds()
          if nextSecond == -59
            nextSecond = 1
          expect(nextSecond).to.equal 1
          expect(nextDate.getMilliseconds()).to.equal 0
          expect(result).to.be.at.most(1000)

      describe 'when called with every 15 seconds option', ->
        it 'should result in a next time of at most 15000 ms', ->
          result = @sut.calculateNextCronInterval "*/15 * * * * *", now
          nextDate = new Date(now.getTime() + result)
          debug 'result from', now, 'to', nextDate.toString(), result, 'ms'
          expect(nextDate.getSeconds() % 15).to.equal 0
          expect(nextDate.getMilliseconds()).to.equal 0
          expect(result).to.be.at.most(15000)

      describe 'when called with minutes option', ->
        it 'should result in a next time of at most 60000 ms', ->
          result = @sut.calculateNextCronInterval "* * * * *", now
          nextDate = new Date(now.getTime() + result)
          debug 'result from', now, 'to', nextDate.toString(), result, 'ms'
          nextMinute = nextDate.getMinutes()- now.getMinutes()
          if nextMinute == -59
            nextMinute = 1
          expect(nextMinute).to.equal 1
          expect(nextDate.getSeconds()).to.equal 0
          expect(nextDate.getMilliseconds()).to.equal 0
          expect(result).to.be.at.most(60000)

      describe 'when called with every 15 minutes option', ->
        it 'should result in a next time of at most 900000 ms', ->
          result = @sut.calculateNextCronInterval "*/15 * * * *", now
          nextDate = new Date(now.getTime() + result)
          debug 'result from', now, 'to', nextDate.toString(), result, 'ms'
          expect(nextDate.getMinutes() % 15).to.equal 0
          expect(nextDate.getSeconds()).to.equal 0
          expect(nextDate.getMilliseconds()).to.equal 0
          expect(result).to.be.at.most(900000)

    describe 'using a fake date', ->
      fakeDate = new Date(2015, 0, 1, 15, 0, 0, 0)

      describe 'when called with seconds option', ->
        it 'should result in a next time of 1000 ms', ->
          result = @sut.calculateNextCronInterval "* * * * * *", fakeDate
          nextDate = new Date(fakeDate.getTime() + result)
          debug 'result from', fakeDate, 'to', new Date(fakeDate.getTime() + result).toString(), result, 'ms'
          nextSecond = nextDate.getSeconds()- fakeDate.getSeconds()
          if nextSecond == -59
            nextSecond = 1
          expect(nextSecond).to.equal 1
          expect(nextDate.getMilliseconds()).to.equal 0
          expect(result).to.equals(1000)

      describe 'when called with every 15 seconds option', ->
        it 'should result in a next time of 15000 ms', ->
          result = @sut.calculateNextCronInterval "*/15 * * * * *", fakeDate
          nextDate = new Date(fakeDate.getTime() + result)
          debug 'result from', fakeDate, 'to', new Date(fakeDate.getTime() + result).toString(), result, 'ms'
          expect(nextDate.getSeconds() % 15).to.equal 0
          expect(nextDate.getMilliseconds()).to.equal 0
          expect(result).to.equals(15000)

      describe 'when called with minutes option', ->
        it 'should result in a next time of 60000 ms', ->
          result = @sut.calculateNextCronInterval "* * * * *", fakeDate
          nextDate = new Date(fakeDate.getTime() + result)
          debug 'result from', fakeDate, 'to', new Date(fakeDate.getTime() + result).toString(), result, 'ms'
          nextMinute = nextDate.getMinutes()- fakeDate.getMinutes()
          if nextMinute == -59
            nextMinute = 1
          expect(nextMinute).to.equal 1
          expect(nextDate.getSeconds()).to.equal 0
          expect(result).to.equals(60000)

      describe 'when called with every 15 minutes option', ->
        it 'should result in a next time of 900000 ms', ->
          result = @sut.calculateNextCronInterval "*/15 * * * *", fakeDate
          nextDate = new Date(fakeDate.getTime() + result)
          debug 'result from', fakeDate, 'to', new Date(fakeDate.getTime() + result).toString(), result, 'ms'
          expect(nextDate.getMinutes() % 15).to.equal 0
          expect(nextDate.getSeconds()).to.equal 0
          expect(nextDate.getMilliseconds()).to.equal 0
          expect(result).to.equals(900000)

      describe 'when called with an invalid cron string', ->
        it 'should throw an error', ->
          hasError = false
          try
            @sut.calculateNextCronInterval "*/1000E * * * * *", fakeDate
          catch error
            hasError = true
          expect(hasError).to.equals true

    describe 'using a fake date with milliseconds set close to an interval', ->
      fakeDate = new Date(2015, 0, 1, 12, 59, 59, 850)

      describe 'when called with seconds option', ->
        it 'should result in a next time of 1150 ms', ->
          result = @sut.calculateNextCronInterval "* * * * * *", fakeDate
          nextDate = new Date(fakeDate.getTime() + result)
          debug 'result from', fakeDate, 'to', new Date(fakeDate.getTime() + result).toString(), result, 'ms'
          nextSecond = nextDate.getSeconds()- fakeDate.getSeconds()
          if nextSecond < 0
            nextSecond = nextSecond + 60
          nextMinute = nextDate.getMinutes()- fakeDate.getMinutes()
          if nextMinute < 0
            nextMinute = nextMinute + 60
          nextHour = nextDate.getHours()- fakeDate.getHours()
          expect(nextSecond).to.equal 2
          expect(nextMinute).to.equal 1
          expect(nextHour).to.equal 1
          expect(nextDate.getMilliseconds()).to.equal 0
          expect(result).to.equals(1150)

      describe 'when called with every 15 seconds option', ->
        it 'should result in a next time of 15150 ms', ->
          result = @sut.calculateNextCronInterval "*/15 * * * * *", fakeDate
          nextDate = new Date(fakeDate.getTime() + result)
          debug 'result from', fakeDate, 'to', new Date(fakeDate.getTime() + result).toString(), result, 'ms'
          expect(nextDate.getSeconds() % 15).to.equal 0
          expect(nextDate.getMilliseconds()).to.equal 0
          expect(result).to.equals(15150)

      describe 'when called with minutes option', ->
        it 'should result in a next time of 60150 ms', ->
          result = @sut.calculateNextCronInterval "* * * * *", fakeDate
          nextDate = new Date(fakeDate.getTime() + result)
          debug 'result from', fakeDate, 'to', new Date(fakeDate.getTime() + result).toString(), result, 'ms'
          nextMinute = nextDate.getMinutes()- fakeDate.getMinutes()
          if nextMinute < 0
            nextMinute = nextMinute + 60
          nextHour = nextDate.getHours()- fakeDate.getHours()
          expect(nextMinute).to.equal 2
          expect(nextHour).to.equal 1
          expect(nextDate.getSeconds()).to.equal 0
          expect(result).to.equals(60150)

      describe 'when called with every 15 minutes option', ->
        it 'should result in a next time of 900150 ms', ->
          result = @sut.calculateNextCronInterval "*/15 * * * *", fakeDate
          nextDate = new Date(fakeDate.getTime() + result)
          debug 'result from', fakeDate, 'to', new Date(fakeDate.getTime() + result).toString(), result, 'ms'
          expect(nextDate.getMinutes() % 15).to.equal 0
          expect(nextDate.getSeconds()).to.equal 0
          expect(nextDate.getMilliseconds()).to.equal 0
          expect(result).to.equals(900150)
