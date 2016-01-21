PingJobProcessor = require '../../src/ping-job-processor'
IORedis = require 'ioredis'
debug = require('debug')('mocha-test')

describe 'PingJobProcessor', ->
  beforeEach ->
    @kue = require 'kue'
    @redis = new IORedis
    @meshbluMessage = message: sinon.stub()
    options = {
      @redis
      @meshbluMessage
      @kue
    }

    @queue = @kue.createQueue
      jobEvents: false

    @sut = new PingJobProcessor options

  describe '->processJob', ->
    beforeEach (done) ->
      @redis.del 'interval:pong:ping-flow-id:some-node-id', done

    beforeEach (done) ->
      @redis.del "interval/job/ping-flow-id/some-node-id", done

    beforeEach (done) ->
      @pingJob = @queue.create 'ping', {sendTo: 'ping-flow-id', nodeId: 'some-node-id'}
      @pingJob.save done

    beforeEach (done) ->
      @intervalJob = @queue.create 'interval', {sendTo: 'ping-flow-id', nodeId: 'some-node-id'}
      @intervalJob.save done

    beforeEach (done) ->
      @redis.sadd "interval/job/ping-flow-id/some-node-id", @intervalJob.id, done

    beforeEach (done) ->
      @redis.sadd "interval/job/ping-flow-id/some-node-id", @pingJob.id, done

    describe 'when called with a job that has not timed out', ->
      beforeEach (done) ->
        @redis.set 'interval:pong:ping-flow-id:some-node-id', Date.now()
        @meshbluMessage.message.yields null
        @sut.processJob @pingJob, {}, done

      it 'should send a message', ->
        message =
          topic: "ping"
          payload:
            from: "some-node-id"

        expect(@meshbluMessage.message).to.have.been.calledWith ['ping-flow-id'], message

    describe 'when called with a job that has timed out', ->
      beforeEach (done) ->
        @meshbluMessage.message.yields null
        @sut.processJob @pingJob, {}, done

      it 'should not send a message', ->
        expect(@meshbluMessage.message).not.to.have.been.called

      it 'should remove the interval job', (done) ->
        @kue.Job.get @intervalJob.id, (error, job) =>
          expect(error).to.exist
          done()

      it 'should remove the ping job', (done) ->
        @kue.Job.get @pingJob.id, (error, job) =>
          expect(error).to.exist
          done()
