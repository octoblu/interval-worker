_ = require 'lodash'
async = require 'async'
UnregisterJobProcessor = require '../src/unregister-job-processor'
redis = require 'fakeredis'
debug = require('debug')('mocha-test')
UUID = require 'uuid'

describe 'UnregisterJobProcessor', ->
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

    @sut = new UnregisterJobProcessor options

  describe '->processJob', ->
    beforeEach (done) ->
      @pingJob = @queue.create 'ping', {sendTo: 'unregister-flow-id', nodeId: 'some-node-id'}
      @pingJob.save done

    beforeEach (done) ->
      @intervalJob = @queue.create 'interval', {sendTo: 'unregister-flow-id', nodeId: 'some-node-id'}
      @intervalJob.save done

    beforeEach (done) ->
      @client.sadd "interval/job/unregister-flow-id/some-node-id", @intervalJob.id, done

    beforeEach (done) ->
      @client.set "interval/nonce/unregister-flow-id/some-node-id", 'i-am-nonce', done

    beforeEach (done) ->
      @client.set "interval/ping/unregister-flow-id/some-node-id", @pingJob.id, done

    context 'with intervalTime', ->
      beforeEach (done) ->
        @unregisterJob = @queue.create 'unregister', {
          sendTo: 'unregister-flow-id'
          nodeId: 'some-node-id'
          intervalTime: 1000
          nonce: 'i-am-nonce'
        }
        @unregisterJob.save done

      beforeEach (done) ->
        @sut.processJob @unregisterJob, {}, done

      it 'should delete the pingJob', (done) ->
        @client.get 'interval/ping/unregister-flow-id/some-node-id', (error, jobId) =>
          return done error if error?
          @kue.Job.get jobId, (error, job) =>
            expect(error).to.exist
            expect(job).not.to.exist
            done()

      it 'should delete the intervalJob', (done) ->
        @client.smembers 'interval/job/unregister-flow-id/some-node-id', (error, jobIds) =>
          return done error if error?
          @kue.Job.get _.first(jobIds), (error, job) =>
            expect(error).to.exist
            expect(job).not.to.exist
            done()

      it 'should remove the interval redis stuff', (done) ->
        async.series {
          active: async.apply @client.exists, 'interval/active/unregister-flow-id/some-node-id'
          time: async.apply @client.exists, 'interval/time/unregister-flow-id/some-node-id'
          cron: async.apply @client.exists, 'interval/cron/unregister-flow-id/some-node-id'
          nonce: async.apply @client.exists, 'interval/nonce/unregister-flow-id/some-node-id'
        }, (error, results) =>
          expect(results.active).to.equal 0
          expect(results.time).to.equal 0
          expect(results.cron).to.equal 0
          expect(results.nonce).to.equal 0
          done()
