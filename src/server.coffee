_ = require 'lodash'
IntervalJobProcessor = require './interval-job-processor'
PingJobProcessor = require './ping-job-processor'
PongJobProcessor = require './pong-job-processor'
RegisterJobProcessor = require './register-job-processor'
debug = require('debug')('nanocyte-interval-service:server')

class Server
  constructor: (@options={},dependencies={})->
    {@intervalTTL,@intervalJobs,@intervalAttempts,@intervalPromotion} = @options
    {@minTimeDiff,@redisPort,@redisHost,@pingInterval} = @options
    debug 'start KueWorker constructor'
    @kue = dependencies.kue ? require 'kue'
    redis = dependencies.redis ? require 'redis'
    MeshbluMessage = dependencies.MeshbluMessage ? require './meshblu-message'
    @client = _.bindAll redis.createClient @redisPort, @redisHost
    @meshbluMessage = new MeshbluMessage

  run: (callback) =>
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
    registerJobProcessor = new RegisterJobProcessor options

    options.registerJobProcessor = registerJobProcessor

    intervalJobProcessor = new IntervalJobProcessor options
    pingJobProcessor = new PingJobProcessor options

    @queue.process 'interval', @intervalJobs, intervalJobProcessor.processJob
    @queue.process 'ping', @intervalJobs, pingJobProcessor.processJob
    @queue.process 'pong', @intervalJobs, pongJobProcessor.processJob
    @queue.process 'register', @intervalJobs, registerJobProcessor.processJob
    callback()

module.exports = Server
