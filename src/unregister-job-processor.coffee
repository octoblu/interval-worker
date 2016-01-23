_          = require 'lodash'
async      = require 'async'
debug      = require('debug')('nanocyte-interval-service:unregister-job-processor')
cronParser = require 'cron-parser'
{Stats}    = require 'fast-stats'

class UnregisterJobProcessor
  constructor: (options) ->
    {@client,@kue,@queue} = options

  processJob: (job, ignore, callback) =>
    debug 'processing unregister job', job.id, 'data', JSON.stringify job.data
    {sendTo, nodeId} = job.data
    @client.get "interval/nonce/#{sendTo}/#{nodeId}", (error, nonce) =>
      return callback error if error?
      return callback new Error 'nonce does not match' unless nonce == job.data.nonce
      async.series [
        async.apply @removeIntervalProperties, {sendTo, nodeId}
        async.apply @removeIntervalJobs, {sendTo, nodeId}
        async.apply @removePingJob, {sendTo, nodeId}
      ], callback

  removeIntervalProperties: ({sendTo, nodeId}, callback) =>
      @client.del "interval/active/#{sendTo}/#{nodeId}",
      "interval/time/#{sendTo}/#{nodeId}",
      "interval/cron/#{sendTo}/#{nodeId}",
      "interval/nonce/#{sendTo}/#{nodeId}", callback

  removeIntervalJobs: ({sendTo, nodeId}, callback) =>
    @client.smembers "interval/job/#{sendTo}/#{nodeId}", (error, jobIds) =>
      return callback error if error?
      async.each jobIds, @removeJob, callback

  removePingJob: ({sendTo, nodeId}, callback) =>
    @client.get "interval/ping/#{sendTo}/#{nodeId}", (error, jobId) =>
      return callback error if error?
      @removeJob jobId, callback

  removeJob: (jobId, callback) =>
    @kue.Job.get jobId, (error, job) =>
      job.remove() unless error?
      callback()

module.exports = UnregisterJobProcessor
