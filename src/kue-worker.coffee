_          = require 'lodash'
async       = require 'async'
cronParser = require 'cron-parser'
debug      = require('debug')('interval-service')

class KueWorker
  constructor: (dependencies={})->
    @INTERVAL_PROMOTION = process.env.INTERVAL_PROMOTION ? 100
    @INTERVAL_JOBS      = process.env.INTERVAL_JOBS ? 1000

    @kue = dependencies.kue ? require 'kue'
    IORedis = dependencies.IORedis ? require 'ioredis'
    MeshbluMessage = dependencies.MeshbluMessage ? require './meshblu-message'
    @redis = new IORedis
    @meshbluMessage = new MeshbluMessage
    @queue = @kue.createQueue promotion: interval: @INTERVAL_PROMOTION

  start: =>
    @queue.process 'interval', @INTERVAL_JOBS, @processJob

  processJob: (job, done=->) =>
    debug 'processing interval job', job.id, 'data', job.data
    jobStartTime = new Date()

    @getTargetJobs job, (error, jobIds) =>
      return done error if error?
      @removeJobs jobIds

      @getJobInfo job, (error, jobInfo) =>
        [ activeGroup, activeTarget, intervalTime, cronString ] = jobInfo?
        debug 'job info', error, jobInfo, job.id
        return done() if !activeGroup or !activeTarget
        debug 'creating a new job!'
        @meshbluMessage.message [job.data.targetId], timestamp: Date.now()

        try
          intervalTime = @calculateNextCronInterval cronString, jobStartTime if cronString?
        catch error
          console.error error
          done()

        @setCronInterval intervalTime, cronString
        done()

  getTargetJobs: (job, callback=->) =>
    @redis.srem "interval/job/#{job.data.targetId}", job.id
    @redis.smembers "interval/job/#{job.data.targetId}", (error, allJobIds) =>
      return callback error if error
      callback null, _.without allJobIds, job.id

  removeJob: (jobId, callback=->) =>
    debug 'removing stale jobId', jobId
    @kue.Job.get jobId, (err, job) =>
      return callback error if err
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
      nextTime = parser.next()
      nextTime.setMilliseconds 0
      timeDiff = nextTime - currentDate
      debug 'this is the next time', timeDiff, nextTime.getTime()

    return timeDiff

  setCronInterval: (intervalTime, cronString) =>
    return unless intervalTime? and cronString?
    @redis.set "interval/time/#{job.data.targetId}", intervalTime

module.exports = KueWorker
