_          = require 'lodash'
async      = require 'async'
debug      = require('debug')('nanocyte-interval-service:interval-job-processor')
cronParser = require 'cron-parser'

class PingJobProcessor
  constructor: (options) ->
    {@meshbluMessage,@redis,@kue} = options

  processJob: (job, ignore, callback) =>
    debug 'processing interval job', job.id, 'data', JSON.stringify job.data
    {sendTo, nodeId} = job.data

    @redis.exists "interval:pong:#{sendTo}:#{nodeId}", (error, extant) =>
      return callback error if error?

      return @_removeJobs {sendTo, nodeId}, callback if extant == 0

      @meshbluMessage.message [sendTo],
        topic: 'ping'
        payload:
          from: nodeId
      , callback

  _removeJobs: ({sendTo, nodeId}, callback) =>
    @redis.smembers "interval/job/#{sendTo}/#{nodeId}", (err, jobIds) =>
      async.eachSeries jobIds, async.apply(@_removeJob), callback

  _removeJob: (jobId, callback) =>
    @kue.Job.get jobId, (error, job) =>
      job.remove() unless error?
      callback()

module.exports = PingJobProcessor
