_ = require 'lodash'
async = require 'async'
PingJobProcessor = require '../../src/ping-job-processor'
redis = require 'fakeredis'
debug = require('debug')('mocha-test')
UUID = require 'uuid'

describe 'PingJobProcessor', ->
  beforeEach ->
    @kue = require 'kue'
    @redisKey = UUID.v1()
    @client = _.bindAll redis.createClient @redisKey
    @meshbluMessage = message: sinon.stub().yields null
    options = {
      @client
      @meshbluMessage
      @kue
      pingInterval: 100000
    }

    @queue = @kue.createQueue
      jobEvents: false

    @sut = new PingJobProcessor options

  beforeEach ->
    @bucket = @sut._getBucket()
    @bucket1 = @sut._getBucket 2
    @bucket2 = @sut._getBucket 3
    @bucket3 = @sut._getBucket 4
    @bucket4 = @sut._getBucket 5
    @bucket5 = @sut._getBucket 6

  describe '->processJob', ->
    beforeEach (done) ->
      @client.del 'interval:pong:ping-flow-id:some-node-id', done

    beforeEach (done) ->
      @client.del "interval/job/ping-flow-id/some-node-id", done

    beforeEach (done) ->
      @pingJob = @queue.create 'ping', {sendTo: 'ping-flow-id', nodeId: 'some-node-id'}
      @pingJob.save done

    beforeEach (done) ->
      @intervalJob = @queue.create 'interval', {sendTo: 'ping-flow-id', nodeId: 'some-node-id'}
      @intervalJob.save done

    beforeEach (done) ->
      @client.sadd "interval/job/ping-flow-id/some-node-id", @intervalJob.id, done

    beforeEach (done) ->
      @client.sadd "interval/ping/ping-flow-id/some-node-id", @pingJob.id, done

    context 'when the system is stable', ->
      beforeEach (done) ->
        @client.hmset "ping:count:#{@bucket}", 'total:ping', 1, 'total:pong', 1, done

      beforeEach (done) ->
        async.series [
          async.apply @client.hmset, "ping:count:#{@bucket1}", 'total:ping', 1, 'total:pong', 1
          async.apply @client.hmset, "ping:count:#{@bucket2}", 'total:ping', 2, 'total:pong', 2
          async.apply @client.hmset, "ping:count:#{@bucket3}", 'total:ping', 3, 'total:pong', 3
          async.apply @client.hmset, "ping:count:#{@bucket4}", 'total:ping', 4, 'total:pong', 4
          async.apply @client.hmset, "ping:count:#{@bucket5}", 'total:ping', 5, 'total:pong', 5
        ], done

      describe 'when called with a job that has not timed out', ->
        beforeEach (done) ->
          @sut.processJob @pingJob, {}, done

        it 'should increment the node ping count', (done) ->
          @client.hget 'ping:count:total', 'ping-flow-id:some-node-id', (error, data) =>
            expect(parseInt(data)).to.equal 1
            done()

        it 'should increment the total ping count', (done) ->
          @client.hget "ping:count:#{@bucket}", 'total:ping', (error, data) =>
            expect(parseInt(data)).to.equal 2
            done()

        it 'should send a message', ->
          message =
            topic: "ping"
            payload:
              from: "some-node-id"

          expect(@meshbluMessage.message).to.have.been.calledWith ['ping-flow-id'], message

      describe 'when called with a job that has timed out', ->
        beforeEach (done) ->
          @client.hset "ping:count:total", 'ping-flow-id:some-node-id', 5, done

        beforeEach (done) ->
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

      describe 'when called with a job that has previously timed out twice, but now works', ->
        beforeEach (done) ->
          @client.hset 'ping:count:total', 'ping-flow-id:some-node-id', 2, done

        beforeEach (done) ->
          @sut.processJob @pingJob, {}, done

        it 'should increment the node ping count from zero', (done) ->
          @client.hget 'ping:count:total', 'ping-flow-id:some-node-id', (error, data) =>
            expect(parseInt(data)).to.equal 1
            done()

        it 'should increment the total ping count', (done) ->
          @client.hget "ping:count:#{@bucket}", 'total:ping', (error, data) =>
            expect(parseInt(data)).to.equal 2
            done()

        it 'should send a message', ->
          message =
            topic: "ping"
            payload:
              from: "some-node-id"

          expect(@meshbluMessage.message).to.have.been.calledWith ['ping-flow-id'], message

    context 'when the system is unstable', ->
      beforeEach (done) ->
        async.series [
          async.apply @client.hmset, "ping:count:#{@bucket1}", 'total:ping', 1, 'total:pong', 1
          async.apply @client.hmset, "ping:count:#{@bucket2}", 'total:ping', 4, 'total:pong', 2
          async.apply @client.hmset, "ping:count:#{@bucket3}", 'total:ping', 8, 'total:pong', 8
          async.apply @client.hmset, "ping:count:#{@bucket4}", 'total:ping', 2, 'total:pong', 1
          async.apply @client.hmset, "ping:count:#{@bucket5}", 'total:ping', 9, 'total:pong', 1
        ], done

      describe 'when called with a job that has not timed out', ->
        beforeEach (done) ->
          @client.hset 'ping:count:total', 'ping-flow-id:some-node-id', 2, done

        beforeEach (done) ->
          @sut.processJob @pingJob, {}, done

        it 'should clear the ping count', (done) ->
          @client.hget 'ping:count:total', 'ping-flow-id:some-node-id', (error, data) =>
            expect(data).to.be.null
            done()

        it 'should increment the total ping count', (done) ->
          @client.hget "ping:count:#{@bucket}", 'total:ping', (error, data) =>
            expect(parseInt(data)).to.equal 1
            done()

        it 'should send a message', ->
          message =
            topic: "ping"
            payload:
              from: "some-node-id"

          expect(@meshbluMessage.message).to.have.been.calledWith ['ping-flow-id'], message

      describe 'when called with a job that has timed out', ->
        beforeEach (done) ->
          @client.hset "ping:count:#{@bucket}", 'ping-flow-id:some-node-id', 5, done

        beforeEach (done) ->
          @sut.processJob @pingJob, {}, done

        it 'should clear the ping count', (done) ->
          @client.hget 'ping:count:total', 'ping-flow-id:some-node-id', (error, data) =>
            expect(data).to.be.null
            done()

        it 'should not remove the interval job', (done) ->
          @kue.Job.get @intervalJob.id, (error, job) =>
            expect(job).to.exist
            done error

  describe '->isSystemStable', ->
    context 'when one pong is zero', ->
      beforeEach (done) ->
        async.series [
          async.apply @client.hmset, "ping:count:#{@bucket1}", 'total:ping', 1, 'total:pong', 0
          async.apply @client.hmset, "ping:count:#{@bucket2}", 'total:ping', 2, 'total:pong', 2
          async.apply @client.hmset, "ping:count:#{@bucket3}", 'total:ping', 3, 'total:pong', 3
          async.apply @client.hmset, "ping:count:#{@bucket4}", 'total:ping', 4, 'total:pong', 4
          async.apply @client.hmset, "ping:count:#{@bucket5}", 'total:ping', 5, 'total:pong', 5
        ], done

      beforeEach (done) ->
        @sut.isSystemStable (error, @stable) => done error

      it 'should be false', ->
        expect(@stable).to.be.false

    context 'when deviation is low', ->
      beforeEach (done) ->
        async.series [
          async.apply @client.hmset, "ping:count:#{@bucket1}", 'total:ping', 1, 'total:pong', 1
          async.apply @client.hmset, "ping:count:#{@bucket2}", 'total:ping', 1, 'total:pong', 1
          async.apply @client.hmset, "ping:count:#{@bucket3}", 'total:ping', 1, 'total:pong', 1
          async.apply @client.hmset, "ping:count:#{@bucket4}", 'total:ping', 1, 'total:pong', 1
          async.apply @client.hmset, "ping:count:#{@bucket5}", 'total:ping', 1, 'total:pong', 1
        ], done

      beforeEach (done) ->
        @sut.isSystemStable (error, @stable) => done error

      it 'should be true', ->
        expect(@stable).to.be.true

    context 'when deviation is high', ->
      beforeEach (done) ->
        async.series [
          async.apply @client.hmset, "ping:count:#{@bucket1}", 'total:ping', 100, 'total:pong', 1
          async.apply @client.hmset, "ping:count:#{@bucket2}", 'total:ping', 100, 'total:pong', 100
          async.apply @client.hmset, "ping:count:#{@bucket3}", 'total:ping', 100, 'total:pong', 100
          async.apply @client.hmset, "ping:count:#{@bucket4}", 'total:ping', 100, 'total:pong', 100
          async.apply @client.hmset, "ping:count:#{@bucket5}", 'total:ping', 100, 'total:pong', 100
        ], done

      beforeEach (done) ->
        @sut.isSystemStable (error, @stable) => done error

      it 'should be false', ->
        expect(@stable).to.be.false

    context 'when deviation is close but still ok', ->
      beforeEach (done) ->
        async.series [
          async.apply @client.hmset, "ping:count:#{@bucket1}", 'total:ping', 100, 'total:pong', 98
          async.apply @client.hmset, "ping:count:#{@bucket2}", 'total:ping', 100, 'total:pong', 99
          async.apply @client.hmset, "ping:count:#{@bucket3}", 'total:ping', 100, 'total:pong', 99
          async.apply @client.hmset, "ping:count:#{@bucket4}", 'total:ping', 100, 'total:pong', 96
          async.apply @client.hmset, "ping:count:#{@bucket5}", 'total:ping', 100, 'total:pong', 100
        ], done

      beforeEach (done) ->
        @sut.isSystemStable (error, @stable) => done error

      it 'should be true', ->
        expect(@stable).to.be.true

    context 'when deviation is close but not ok', ->
      beforeEach (done) ->
        async.series [
          async.apply @client.hmset, "ping:count:#{@bucket1}", 'total:ping', 100, 'total:pong', 98
          async.apply @client.hmset, "ping:count:#{@bucket2}", 'total:ping', 100, 'total:pong', 99
          async.apply @client.hmset, "ping:count:#{@bucket3}", 'total:ping', 100, 'total:pong', 99
          async.apply @client.hmset, "ping:count:#{@bucket4}", 'total:ping', 100, 'total:pong', 95
          async.apply @client.hmset, "ping:count:#{@bucket5}", 'total:ping', 100, 'total:pong', 100
        ], done

      beforeEach (done) ->
        @sut.isSystemStable (error, @stable) => done error

      it 'should be false', ->
        expect(@stable).to.be.false