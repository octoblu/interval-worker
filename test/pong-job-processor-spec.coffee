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

    options = {
      @client
    }

    @sut = new PongJobProcessor options

  describe '->processJob', ->
    beforeEach (done) ->
      @pongJob = @queue.create 'pong', {sendTo: 'some-flow-id', nodeId: 'some-node-id', bucket: 'the-bucket'}
      @pongJob.save done

    beforeEach (done) ->
      @sut.processJob @pongJob, {}, done

    it 'should set the pong count', (done) ->
      @client.hget 'ping:count:the-bucket', 'total:pong', (error, data) =>
        expect(parseInt(data)).to.equal 1
        done()
