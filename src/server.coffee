_ = require 'lodash'
IntervalJobProcessor = require './interval-job-processor'
PingJobProcessor = require './ping-job-processor'
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

    intervalJobProcessor = new IntervalJobProcessor {
      @pingInterval
      @intervalTTL
      @minTimeDiff
      @intervalAttempts
      @client
      @meshbluMessage
      @queue
      @kue
    }

    pingJobProcessor = new PingJobProcessor {
      @pingInterval
      @client
      @meshbluMessage
      @kue
    }

    @queue.process 'interval', @intervalJobs, intervalJobProcessor.processJob
    @queue.process 'ping', @intervalJobs, pingJobProcessor.processJob
    callback()

module.exports = Server
