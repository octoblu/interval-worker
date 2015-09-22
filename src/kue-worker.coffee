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

    @kue = dependencies.kue ? require 'kue'
    IORedis = dependencies.IORedis ? require 'ioredis'
    MeshbluMessage = dependencies.MeshbluMessage ? require './meshblu-message'
    @redis = new IORedis port: process.env.REDIS_PORT, host: process.env.REDIS_HOST
    @meshbluMessage = new MeshbluMessage
    @queue = @kue.createQueue promotion: interval: @INTERVAL_PROMOTION
    debug 'done KueWorker constructor'

  start: =>
    debug 'kueWorker queue start'
    @queue.process 'interval', @INTERVAL_JOBS, @processJob

  processJob: (job, ctx, done) =>
    debug 'processing interval job', job.id, 'data', job.data
    jobStartTime = new Date()

    @getTargetJobs job, (error, jobIds) =>
      return done error if error?
      @removeJobs jobIds

      @getJobInfo job, (error, jobInfo) =>
        [ activeGroup, activeTarget, intervalTime, cronString ] = jobInfo
        debug 'job info', error, jobInfo, job.id
        return done() if !activeGroup or !activeTarget
        debug 'creating a new job!'
        @meshbluMessage.message [job.data.targetId], timestamp: Date.now()

        if cronString
          debug 'calculating next interval from cronString', cronString
          try
            intervalTime = @calculateNextCronInterval cronString, jobStartTime
            @redis.set "interval/time/#{job.data.targetId}", intervalTime
          catch error
            console.error error
            done()

        @createJob job.data, intervalTime, (err, newJob) =>
          debug 'created a job', newJob.id, 'with intervalTime', intervalTime
          @redis.sadd "interval/job/#{job.data.targetId}", newJob.id
          done(err)

  getTargetJobs: (job, callback=->) =>
    @redis.srem "interval/job/#{job.data.targetId}", job.id
    @redis.smembers "interval/job/#{job.data.targetId}", (error, allJobIds) =>
      return callback error if error
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
      "interval/active/#{job.data.groupId}",
      "interval/active/#{job.data.targetId}",
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
      save (error) =>
        callback error, job

module.exports = KueWorker
