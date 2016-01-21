IntervalJobProcessor = require './interval-job-processor'
PingJobProcessor = require './ping-job-processor'
debug = require('debug')('nanocyte-interval-service:server')

class Server
  constructor: (@options={},dependencies={})->
    {@intervalTTL,@intervalJobs,@intervalAttempts,@intervalPromotion} = @options
    {@minTimeDiff,@redisPort,@redisHost} = @options
    debug 'start KueWorker constructor'
    @kue = dependencies.kue ? require 'kue'
    IORedis = dependencies.IORedis ? require 'ioredis'
    MeshbluMessage = dependencies.MeshbluMessage ? require './meshblu-message'
    @redis = new IORedis @redisPort, @redisHost
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

    intervalJobProcessor = new IntervalJobProcessor {@intervalTTL,@minTimeDiff,@intervalAttempts,@redis,@meshbluMessage,@queue,@kue}
    pingJobProcessor = new PingJobProcessor {@redis,@meshbluMessage,@kue}

    @queue.process 'interval', @intervalJobs, intervalJobProcessor.processJob
    @queue.process 'ping', @intervalJobs, pingJobProcessor.processJob
    callback()

module.exports = Server
