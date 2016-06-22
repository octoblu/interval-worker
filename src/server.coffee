_ = require 'lodash'
IntervalJobProcessor = require './interval-job-processor'
PingJobProcessor = require './ping-job-processor'
PongJobProcessor = require './pong-job-processor'
RegisterJobProcessor = require './register-job-processor'
UnregisterJobProcessor = require './unregister-job-processor'
debug = require('debug')('nanocyte-interval-service:server')

class Server
  constructor: (@options={},dependencies={})->
    {@intervalTTL,@intervalJobs,@intervalAttempts,@intervalPromotion} = @options
    {@minTimeDiff,@redisPort,@redisHost,@pingInterval} = @options
    debug 'start KueWorker constructor'
    @kue = dependencies.kue ? require 'kue'
    redis = dependencies.redis ? require 'ioredis'
    MeshbluMessage = dependencies.MeshbluMessage ? require './meshblu-message'
    @client = _.bindAll redis.createClient @redisPort, @redisHost, dropBufferSupport: true
    @meshbluMessage = new MeshbluMessage

  writeTest: =>
    @client.set 'test:write', Date.now(), (error) =>
      if error?
        console.error error.stack
        console.log "Write failed, exiting..."
        process.exit 1

  run: (callback) =>
    setInterval @writeTest, 5000

    @queue = @kue.createQueue
      jobEvents: false
      redis:
        port: @redisPort
        host: @redisHost
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
      @meshbluMessage
      @queue
      @kue
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

module.exports = Server
