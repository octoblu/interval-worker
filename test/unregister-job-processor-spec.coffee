_ = require 'lodash'
async = require 'async'
RegisterJobProcessor = require '../src/register-job-processor'
UnregisterJobProcessor = require '../src/unregister-job-processor'
redis = require 'fakeredis'
debug = require('debug')('mocha-test')
UUID = require 'uuid'

describe 'UnregisterJobProcessor', ->
  beforeEach ->
    @kue = require 'kue'
    @redisKey = UUID.v1()
    @client = redis.createClient @redisKey
    @client = _.bindAll @client, _.functionsIn(@client)

    @queue = @kue.createQueue
      jobEvents: false
      redis:
        createClientFactory: =>
         redis.createClient @redisKey

    options = {
      @client
      @kue
      pingInterval: 100000
      intervalTTL: 1000
      intervalAttempts: 3
      minTimeDiff: 500
      @queue
    }

    registerJobProcessor = new RegisterJobProcessor options
    options.registerJobProcessor = registerJobProcessor

    @sut = new UnregisterJobProcessor options

  describe '->processJob', ->
    beforeEach (done) ->
      @pingJob = @queue.create 'ping', {sendTo: 'unregister-flow-id', nodeId: 'some-node-id'}
      @pingJob.events(false).save done

    beforeEach (done) ->
      @intervalJob = @queue.create 'interval', {sendTo: 'unregister-flow-id', nodeId: 'some-node-id'}
      @intervalJob.events(false).save done

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
        @unregisterJob.events(false).save done

      beforeEach (done) ->
        @sut.processJob @unregisterJob, {}, done

      it 'should delete the pingJob', (done) ->
          @kue.Job.get @pingJob.id, (error, job) =>
            expect(error).to.exist
            expect(job).not.to.exist
            done()

      it 'should delete the intervalJob', (done) ->
          @kue.Job.get @intervalJob.id, (error, job) =>
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

    context 'when nonce does not match', ->
      beforeEach (done) ->
        @unregisterJob = @queue.create 'unregister', {
          sendTo: 'unregister-flow-id'
          nodeId: 'some-node-id'
          intervalTime: 1000
          nonce: 'i-am-not-nonce'
        }
        @unregisterJob.events(false).save done

      beforeEach (done) ->
        @sut.processJob @unregisterJob, {}, (@error) => done()

      it 'should yield an error', ->
        expect(@error).to.exist
