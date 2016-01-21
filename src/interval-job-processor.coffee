_          = require 'lodash'
async      = require 'async'
debug      = require('debug')('nanocyte-interval-service:interval-job-processor')
cronParser = require 'cron-parser'

class IntervalJobProcessor
  constructor: (options,dependencies={}) ->
    {@kue,@minTimeDiff,@intervalAttempts,@intervalTTL,@redis,@meshbluMessage,@queue,@pingInterval} = options

  getJobs: (job, callback) =>
    key = "interval/job/#{job.data.sendTo}/#{job.data.nodeId}"

    @redis.srem key, job.id, (error) =>
      return callback error if error?
      @redis.smembers key, (error, allJobIds) =>
        return callback error if error?
        callback null, allJobIds

  removeJob: (jobId, callback) =>
    @kue.Job.get jobId, (error, job) =>
      job.remove() unless error?
      callback()

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

    while timeDiff <= @minTimeDiff
      nextDate = parser.next()
      nextDate.setMilliseconds 0
      timeDiff = nextDate - currentDate

    return timeDiff

  createJob: (data, intervalTime, callback)=>
    job = @queue.create('interval', data).
      delay(intervalTime).
      removeOnComplete(true).
      attempts(@intervalAttempts).
      ttl(@intervalTTL).
      save (err) =>
        callback err, job

  createPingJob: (data, callback) =>
    data = _.clone data
    data.noUnsubscribe = true
    job = @queue.create('ping', data).
      delay(@pingInterval).
      removeOnComplete(true).
      attempts(@intervalAttempts).
      ttl(@intervalTTL).
      save (err) =>
        callback err, job

  processJob: (job, ignore, done) =>
    debug 'processing interval job', job.id, 'data', JSON.stringify job.data
    jobStartTime = new Date()
    if (!job?.data?.sendTo?) or (!job?.data?.nodeId?)
      return done()

    @getJobs job, (err, jobIds) =>
      return done err if err?
      @removeJobs jobIds unless job.data.noUnsubscribe

      @getJobInfo job, (err, jobInfo) =>
        return done err if err?
        [ active, intervalTime, cronString ] = jobInfo

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
          catch error
            console.error error
            done()

        jobKey = "interval/job/#{job.data.sendTo}/#{job.data.nodeId}"

        @createJob job.data, intervalTime, (error, newJob) =>
          return done error if error?
          @redis.sadd jobKey, newJob.id, (error) =>
            return done error if error?
            @createPingJob job.data, (error, pingJob) =>
              return done error if error?
              @redis.sadd jobKey, pingJob.id, done

module.exports = IntervalJobProcessor
