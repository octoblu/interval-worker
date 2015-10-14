_          = require 'lodash'
async      = require 'async'
debug      = require('debug')('nanocyte-interval-service:kue-worker')
cronParser = require 'cron-parser'

class KueWorker
  constructor: (dependencies={})->
    @INTERVAL_TTL       = process.env.INTERVAL_TTL ? 10000
    @INTERVAL_JOBS      = process.env.INTERVAL_JOBS ? 1000
    @INTERVAL_ATTEMPTS  = process.env.INTERVAL_ATTEMPTS ? 999
    @INTERVAL_PROMOTION = process.env.INTERVAL_PROMOTION ? 100
    @REDIS_PORT         = process.env.REDIS_PORT ? 6379
    @REDIS_HOST         = process.env.REDIS_HOST ? 'localhost'

    @kue = dependencies.kue ? require 'kue'
    IORedis = dependencies.IORedis ? require 'ioredis'
    MeshbluMessage = dependencies.MeshbluMessage ? require './meshblu-message'
    @redis = new IORedis @REDIS_PORT, @REDIS_HOST
    @meshbluMessage = new MeshbluMessage

    @queue = @kue.createQueue
      redis:
        port: @REDIS_PORT
        host: @REDIS_HOST
      promotion:
        interval: @INTERVAL_PROMOTION

  start: =>
    @queue.process 'interval', @INTERVAL_JOBS, @processJob

  processJob: (job, ctx, done) =>
    debug 'processing interval job', job.id, 'data', JSON.stringify job.data
    jobStartTime = new Date()

    if (!job?.data?.sendTo?) or (!job?.data?.nodeId?)
      return done()

    @getJobs job, (err, jobIds) =>
      return done err if err?
      @removeJobs jobIds unless job.data.noUnsubscribe

      @getJobInfo job, (err, jobInfo) =>
        [ active, intervalTime, cronString ] = jobInfo
        return done err if err?

        if !active or (_.isNaN(Number intervalTime) and _.isEmpty cronString)
          return done()

        @meshbluMessage.message [job.data.sendTo],
          payload:
            from: job.data.nodeId
            timestamp: _.now()

        return done() if job.data.fireOnce

        if cronString
          try
            intervalTime = @calculateNextCronInterval cronString, jobStartTime
            @redis.set "interval/time/#{job.data.sendTo}/#{job.data.nodeId}", intervalTime
          catch err
            console.err err
            done()

        @createJob job.data, intervalTime, (err, newJob) =>
          @redis.sadd "interval/job/#{job.data.sendTo}/#{job.data.nodeId}", newJob.id
          done(err)

  getJobs: (job, callback=->) =>
    @redis.srem "interval/job/#{job.data.sendTo}/#{job.data.nodeId}", job.id
    @redis.smembers "interval/job/#{job.data.sendTo}/#{job.data.nodeId}", (err, allJobIds) =>
      return callback err if err
      callback null, _.without allJobIds, job.id

  removeJob: (jobId, callback=->) =>
    @kue.Job.get jobId, (err, job) =>
      return callback err if err
      job.remove()
      callback null

  removeJobs: (jobIds, callback=->)=>
    async.each jobIds, @removeJob, callback

  getJobInfo: (job, callback=->) =>
    keys = [
      "interval/active/#{job.data.sendTo}/#{job.data.nodeId}",
      "interval/time/#{job.data.sendTo}/#{job.data.nodeId}",
      "interval/cron/#{job.data.sendTo}/#{job.data.nodeId}"
    ]
    @redis.mget keys, callback

  calculateNextCronInterval: (cronString, currentDate) =>
    currentDate ?= new Date
    timeDiff = 0
    parser = cronParser.parseExpression cronString, currentDate: currentDate

    while timeDiff <= 0
      nextDate = parser.next()
      nextDate.setMilliseconds 0
      timeDiff = nextDate - currentDate

    return timeDiff

  createJob: (data, intervalTime, callback)=>
    job = @queue.create('interval', data).
      delay(intervalTime).
      removeOnComplete(true).
      attempts(@INTERVAL_ATTEMPTS).
      ttl(@INTERVAL_TTL).
      save (err) =>
        callback err, job

module.exports = KueWorker
