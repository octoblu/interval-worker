_                      = require 'lodash'
IntervalJobProcessor   = require './interval-job-processor'
PingJobProcessor       = require './ping-job-processor'
PongJobProcessor       = require './pong-job-processor'
RegisterJobProcessor   = require './register-job-processor'
UnregisterJobProcessor = require './unregister-job-processor'
debug                  = require('debug')('nanocyte-interval-service:server')
Redis                  = require 'ioredis'
MeshbluConfig          = require 'meshblu-config'

class IntervalWorker
  constructor: (@options={},dependencies={})->
    {
      @intervalTTL
      @intervalJobs
      @intervalAttempts
      @intervalPromotion
      @minTimeDiff
      @redisUri
      @pingInterval
    } = @options
    debug 'start KueWorker constructor'
    @kue = dependencies.kue ? require 'kue'

  writeTest: =>
    @client.set 'test:write', Date.now(), (error) =>
      if error?
        console.error 'writeTest', error.stack
        console.log "Write failed, exiting..."
        process.exit 1

  run: (callback) =>
    callback = _.once callback
    @client = new Redis @redisUri, dropBufferSupport: true
    @client = _.bindAll @client, _.functionsIn(@client)
    @client.on 'error', callback
    @client.on 'ready', =>
      setInterval @writeTest, 5000

      @queue = @kue.createQueue
        jobEvents: false
        redis:
          createClientFactory: =>
            new Redis @redisUri, dropBufferSupport: true
        promotion:
          interval: @intervalPromotion

      @queue.watchStuckJobs()
      debug 'kueWorker queue start'

      options = {
        @pingInterval
        @intervalTTL
        @minTimeDiff
        @intervalAttempts
        @client
        @queue
        @kue
        meshbluConfig: new MeshbluConfig().toJSON()
      }
      debug {@pingInterval}
      debug {@intervalTTL}
      debug {@minTimeDiff}

      registerJobProcessor = new RegisterJobProcessor options
      options.registerJobProcessor = registerJobProcessor

      intervalJobProcessor = new IntervalJobProcessor options
      pingJobProcessor = new PingJobProcessor options
      pongJobProcessor = new PongJobProcessor options
      unregisterJobProcessor = new UnregisterJobProcessor options

      @queue.on 'error', (error) =>
        console.error 'Queue error:', error

      @queue.process 'interval', @intervalJobs, intervalJobProcessor.processJob
      @queue.process 'ping', @intervalJobs, pingJobProcessor.processJob
      @queue.process 'pong', @intervalJobs, pongJobProcessor.processJob
      @queue.process 'register', @intervalJobs, registerJobProcessor.processJob
      @queue.process 'unregister', @intervalJobs, unregisterJobProcessor.processJob
      callback()

module.exports = IntervalWorker
