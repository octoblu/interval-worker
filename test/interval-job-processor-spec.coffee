_ = require 'lodash'
IntervalJobProcessor = require '../src/interval-job-processor'
RegisterJobProcessor = require '../src/register-job-processor'
redis = require 'fakeredis'
debug = require('debug')('mocha-test')
async = require 'async'
UUID = require 'uuid'

describe 'IntervalJobProcessor', ->
  beforeEach ->
    @kue = require 'kue'
    @redisKey = UUID.v1()
    @client = redis.createClient @redisKey
    @client = _.bindAll @client, _.functionsIn(@client)
    @meshbluMessage = message: sinon.stub()
    dependencies = {}

    @queue = @kue.createQueue
      jobEvents: false
      redis:
        createClientFactory: =>
         redis.createClient @redisKey

    options = {
      minTimeDiff : 150
      @client
      @meshbluMessage
      @kue
      @queue
    }

    registerJobProcessor = new RegisterJobProcessor options
    options.registerJobProcessor = registerJobProcessor
    @sut = new IntervalJobProcessor options

  beforeEach (done) ->
    @intervalJob = @queue.create 'interval', {sendTo: 'some-flow-id', nodeId: 'some-node-id', intervalTime: 1000}
    @intervalJob.save done

  beforeEach (done) ->
    @client.sadd "interval/job/some-flow-id/some-node-id", @intervalJob.id, done

  beforeEach (done) ->
    async.series [
      async.apply @client.set, "interval/active/some-flow-id/some-node-id", 'true'
      async.apply @client.set, "interval/time/some-flow-id/some-node-id", 1000
      # async.apply @client.set, "interval/cron/some-flow-id/some-node-id", 'cronString'
    ], done

  describe '->processJob', ->
    describe 'when called with a job', ->
      beforeEach (done) ->
        @sut.processJob @intervalJob, {}, done

      it 'should add a job', (done) ->
        @client.exists 'interval/job/some-flow-id/some-node-id', (error, record) =>
          expect(record).to.equal 1
          done error

      it 'should send a message', ->
        expect(@meshbluMessage.message).to.have.been.called

    describe 'when called with a ping:disabled job', ->
      beforeEach (done) ->
        @client.hset 'ping:disabled', 'some-flow-id:some-node-id', Date.now(), done

      beforeEach (done) ->
        @sut.processJob @intervalJob, {}, done

      it 'should add a job', (done) ->
        @client.exists 'interval/job/some-flow-id/some-node-id', (error, record) =>
          expect(record).to.equal 1
          done error

      it 'should not send a message', ->
        expect(@meshbluMessage.message).not.to.have.been.called

  describe '->getJobs', ->
    describe 'when called with a job', ->
      beforeEach (done) ->
        @sut.getJobs @intervalJob, (error, @jobs) => done error

      it 'should return a list of jobs', ->
        expect(@jobs).to.deep.equal []

  describe '->removeJob', ->
    describe 'when called with a job', ->
      beforeEach (done) ->
        @sut.removeJob @intervalJob.id, done

      it 'should remove the interval job', (done) ->
        @kue.Job.get @intervalJob.id, (error, job) =>
          expect(error).to.exist
          done()

  describe '->getJobInfo', ->
    describe 'when called with a job', ->
      beforeEach (done) ->
        @sut.getJobInfo @intervalJob, (error, @jobInfo) => done error

      it 'should yield jobInfo', ->
        jobInfo = ['true', '1000', null, null]
        expect(@jobInfo).to.deep.equal jobInfo
