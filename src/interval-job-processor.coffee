_           = require 'lodash'
async       = require 'async'
MeshbluHttp = require 'meshblu-http'
debug       = require('debug')('nanocyte-interval-service:interval-job-processor')

class IntervalJobProcessor
  constructor: (options, dependencies={}) ->
    {
      @kue
      @client
      @queue
      @registerJobProcessor
      @meshbluConfig
    } = options
    @MeshbluHttp = dependencies.MeshbluHttp ? MeshbluHttp

  getJobs: (job, callback) =>
    key = "interval/job/#{job.data.sendTo}/#{job.data.nodeId}"

    @client.srem key, job.id, (error) =>
      return callback error if error?
      @client.smembers key, callback

  removeJob: (jobId, callback) =>
    return callback() unless jobId?
    @kue.Job.get jobId, (error, job) =>
      return callback() if error?
      job.remove =>
        callback()

  removeJobs: (jobIds, callback) =>
    async.each jobIds, @removeJob, callback

  getJobInfo: (job, callback) =>
    {sendTo, nodeId} = job.data

    keys = [
      "interval/active/#{sendTo}/#{nodeId}",
      "interval/time/#{sendTo}/#{nodeId}",
      "interval/cron/#{sendTo}/#{nodeId}"
      "interval/nonce/#{sendTo}/#{nodeId}"
      "interval/uuid/#{sendTo}/#{nodeId}"
      "interval/token/#{sendTo}/#{nodeId}"
    ]
    @client.mget keys, (error, results) =>
      return callback error if error?
      results = _.map results, (data) =>
        return undefined if _.isEmpty data
        data

      callback null, results

  createJob: (data, intervalTime, callback)=>
    job = @queue.create('interval', data).
      delay(intervalTime).
      removeOnComplete(true).
      attempts(@intervalAttempts).
      ttl(@intervalTTL).
      save (error) =>
        callback error, job

  removeJobsIfNoUnsubscribe: (job, callback) =>
    return callback() if job.data.noUnsubscribe
    @getJobs job, (error, jobIds) =>
      return callback error if error?
      @removeJobs jobIds, callback

  processJob: (job, ignore, callback) =>
    debug 'processing interval job', job.id, 'data', JSON.stringify job.data
    if (!job?.data?.sendTo?) or (!job?.data?.nodeId?)
      return callback()

    @removeJobsIfNoUnsubscribe job, (error) =>
      return callback error if error?

      {sendTo, nodeId, fireOnce} = job.data
      @client.hexists 'ping:disabled', "#{sendTo}:#{nodeId}", (error, disabled) =>
        return callback error if error?

        @getJobInfo job, (error, jobInfo) =>
          return callback error if error?
          [ active, intervalTime, cronString, nonce, uuid, token ] = jobInfo

          if !active or (_.isNaN(Number intervalTime) and _.isEmpty cronString)
            return callback()

          unless disabled
            config = _.defaults {uuid, token}, @meshbluConfig
            meshbluHttp = new @MeshbluHttp config
            message =
              devices: [sendTo]
              payload:
                from: nodeId
                timestamp: _.now()

            meshbluHttp.message message

            console.log {config, message}

          return callback() if fireOnce

          data = _.clone job.data
          data.intervalTime = intervalTime
          data.cronString = cronString
          data.nonce = nonce

          @registerJobProcessor.createIntervalJob data, callback

module.exports = IntervalJobProcessor
