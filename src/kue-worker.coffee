_          = require 'lodash'
async      = require 'async'
debug      = require('debug')('interval-service')
cronParser = require 'cron-parser'

class KueWorker
  constructor: (dependencies={})->
    debug 'start KueWorker constructor'

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

    debug 'done KueWorker constructor'

  start: =>
    debug 'kueWorker queue start'
    @queue.process 'interval', @INTERVAL_JOBS, @processJob

  processJob: (job, ctx, done) =>
    debug 'processing interval job', job.id, 'data', job.data
    jobStartTime = new Date()

    @getTargetJobs job, (err, jobIds) =>
      return done err if err?
      @removeJobs jobIds

      @getJobInfo job, (err, jobInfo) =>
        [ activeTarget, fromId, intervalTime, cronString ] = jobInfo
        debug 'job info', err, jobInfo, job.id
        return done err if err?

        if !activeTarget or (_.isNaN(Number intervalTime) and _.isEmpty cronString)
          debug 'aborting job!'
          return done()

        debug 'creating a new job!'
        @meshbluMessage.message [job.data.targetId], payload: from: fromId

        if cronString
          debug 'calculating next interval from cronString', cronString
          try
            intervalTime = @calculateNextCronInterval cronString, jobStartTime
            @redis.set "interval/time/#{job.data.targetId}", intervalTime
          catch err
            console.err err
            done()

        @createJob job.data, intervalTime, (err, newJob) =>
          debug 'created a job', newJob.id, 'with intervalTime', intervalTime
          @redis.sadd "interval/job/#{job.data.targetId}", newJob.id
          done(err)

  getTargetJobs: (job, callback=->) =>
    @redis.srem "interval/job/#{job.data.targetId}", job.id
    @redis.smembers "interval/job/#{job.data.targetId}", (err, allJobIds) =>
      return callback err if err
      callback null, _.without allJobIds, job.id

  removeJob: (jobId, callback=->) =>
    debug 'removing stale jobId', jobId
    @kue.Job.get jobId, (err, job) =>
      return callback err if err
      job.remove()
      callback null

  removeJobs: (jobIds, callback=->)=>
    async.each jobIds, @removeJob, callback

  getJobInfo: (job, callback=->) =>
    keys = [
      "interval/active/#{job.data.targetId}",
      "interval/fromId/#{job.data.targetId}",
      "interval/time/#{job.data.targetId}",
      "interval/cron/#{job.data.targetId}"
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
      debug 'this is the next time', timeDiff, nextDate.getTime()

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
