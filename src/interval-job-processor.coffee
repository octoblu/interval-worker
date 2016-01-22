_          = require 'lodash'
async      = require 'async'
debug      = require('debug')('nanocyte-interval-service:interval-job-processor')
cronParser = require 'cron-parser'

class IntervalJobProcessor
  constructor: (options,dependencies={}) ->
    {@kue,@minTimeDiff,@intervalAttempts,@intervalTTL,@client,@meshbluMessage,@pingInterval,@queue} = options

  getJobs: (job, callback) =>
    key = "interval/job/#{job.data.sendTo}/#{job.data.nodeId}"

    @client.srem key, job.id, (error) =>
      return callback error if error?
      @client.smembers key, callback

  removeJob: (jobId, callback) =>
    @kue.Job.get jobId, (error, job) =>
      job.remove() unless error?
      callback()

  removeJobs: (jobIds, callback) =>
    async.each jobIds, @removeJob, callback

  getJobInfo: (job, callback) =>
    {sendTo, nodeId} = job.data

    keys = [
      "interval/active/#{sendTo}/#{nodeId}",
      "interval/time/#{sendTo}/#{nodeId}",
      "interval/cron/#{sendTo}/#{nodeId}"
    ]
    @client.mget keys, callback

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
      save (error) =>
        callback error, job

  createPingJob: (data, callback) =>
    {sendTo, nodeId} = data
    pingJobKey = "interval/ping/#{sendTo}/#{nodeId}"
    @client.get pingJobKey, (error, pingJobId) =>
      return callback error if error?
      @kue.Job.get pingJobId, (ignoreError, job) =>
        return callback job if job?

        job = @queue.create('ping', data).
          delay(@pingInterval).
          removeOnComplete(true).
          attempts(@intervalAttempts).
          ttl(@intervalTTL).
          save (error) =>
            return callback error if error?
            @client.set pingJobKey, job.id, (error) =>
              callback error, job

  removeJobsIfnoUnsubscribe: (job, callback) =>
    return callback() if job.data.noUnsubscribe
    @getJobs job, (error, jobIds) =>
      return callback error if error?
      @removeJobs jobIds, callback

  processJob: (job, ignore, callback) =>
    debug 'processing interval job', job.id, 'data', JSON.stringify job.data
    jobStartTime = new Date()
    if (!job?.data?.sendTo?) or (!job?.data?.nodeId?)
      return callback()

    @removeJobsIfnoUnsubscribe job, (error) =>
      return callback error if error?

      {sendTo, nodeId, fireOnce} = job.data
      @client.hexists 'ping:disabled', "#{sendTo}:#{nodeId}", (error, disabled) =>
        return callback error if error?

        @getJobInfo job, (error, jobInfo) =>
          return callback error if error?
          [ active, intervalTime, cronString ] = jobInfo

          if !active or (_.isNaN(Number intervalTime) and _.isEmpty cronString)
            return callback()

          unless disabled
            @meshbluMessage.message [sendTo],
              payload:
                from: nodeId
                timestamp: _.now()

          return callback() if fireOnce

          if cronString
            try
              intervalTime = @calculateNextCronInterval cronString, jobStartTime
              @client.set "interval/time/#{sendTo}/#{nodeId}", intervalTime
            catch error
              console.error error
              return callback()

          jobKey = "interval/job/#{sendTo}/#{nodeId}"

          _.delay =>
            @createJob job.data, intervalTime, (error, newJob) =>
              return callback error if error?
              @client.sadd jobKey, newJob.id, (error) =>
                return callback error if error?
                @createPingJob job.data, callback

module.exports = IntervalJobProcessor
