_ = require 'lodash'
PongJobProcessor = require '../src/pong-job-processor'
RegisterJobProcessor = require '../src/register-job-processor'
redis = require 'fakeredis'
debug = require('debug')('mocha-test')
UUID = require 'uuid'

describe 'PongJobProcessor', ->
  beforeEach ->
    @kue = require 'kue'
    @redisKey = UUID.v1()
    @client = _.bindAll redis.createClient @redisKey

    @queue = @kue.createQueue
      jobEvents: false
      redis:
        createClientFactory: =>
         redis.createClient @redisKey

    options = {
      @client
    }

    @sut = new PongJobProcessor options

  describe '->processJob', ->
    context 'when an interval is ok', ->
      beforeEach (done) ->
        @pongJob = @queue.create 'pong', {sendTo: 'some-flow-id', nodeId: 'some-node-id', bucket: 'the-bucket'}
        @pongJob.save done

      beforeEach (done) ->
        @sut.processJob @pongJob, {}, done

      it 'should set the bucket pong count', (done) ->
        @client.hget 'ping:count:the-bucket', 'total:pong', (error, data) =>
          expect(parseInt(data)).to.equal 1
          done()

      it 'should reset the interval ping count', (done) ->
        @client.hexists 'ping:count:total', 'some-flow-id:some-node-id', (error, data) =>
          expect(data).to.equal 0
          done()

    context 'when an interval has previously failed, but now passes', ->
      beforeEach (done) ->
        @client.hset 'ping:count:total', 'some-flow-id:some-node-id', 2, done

      beforeEach (done) ->
        @pongJob = @queue.create 'pong', {sendTo: 'some-flow-id', nodeId: 'some-node-id', bucket: 'the-bucket'}
        @pongJob.save done

      beforeEach (done) ->
        @sut.processJob @pongJob, {}, done

      it 'should reset all ping counts', (done) ->
        @client.exists 'ping:count:total', (error, data) =>
          expect(data).to.equal 0
          done()
