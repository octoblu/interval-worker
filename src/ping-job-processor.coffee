_          = require 'lodash'
async      = require 'async'
debug      = require('debug')('nanocyte-interval-service:ping-job-processor')
cronParser = require 'cron-parser'
{Stats}    = require 'fast-stats'

class PingJobProcessor
  constructor: (options) ->
    {@meshbluMessage,@client,@kue,@pingInterval,@queue,@registerJobProcessor} = options

  processJob: (job, ignore, callback) =>
    debug 'processing ping job', job.id, 'data', JSON.stringify job.data
    {sendTo, nodeId} = job.data

    flowNodeKey = "#{sendTo}:#{nodeId}"
    bucket = @_getBucket()

    @isIntervalAvailable {sendTo, nodeId}, (error, intervalAvailable) =>
      return callback error if error?
      return callback unless intervalAvailable

      @isSystemStable (error, systemStable) =>
        return callback error if error?

        @clearIfUnstable systemStable, (error) =>
          return callback error if error?

          @client.hget "ping:count:total", flowNodeKey, (error, count) =>
            return callback error if error?

            count ?= 0
            unitStable = count <= 1

            @clearIfUnstable unitStable, (error) =>
              return callback error if error?

              if systemStable && parseInt(count || 0) >= 5
                return @_disableJobs({pingJobId: job.id, sendTo, nodeId}, callback)

              message =
                  topic: 'ping'
                  payload:
                    from: nodeId
                    nodeId: nodeId
                    bucket: @_getBucket()
                    timestamp: _.now()

              tasks = [
                async.apply @client.hincrby, "ping:count:#{bucket}", 'total:ping', 1
                async.apply @meshbluMessage.message, [sendTo], message
                async.apply @registerJobProcessor.createPingJob, job.data
              ]

              if systemStable
                tasks.push async.apply @client.hincrby, 'ping:count:total', flowNodeKey, 1

              async.series tasks, callback

  _disableJobs: ({pingJobId, sendTo, nodeId}, callback) =>
    @client.smembers "interval/job/#{sendTo}/#{nodeId}", (err, jobIds) =>
      jobIds ?= []
      async.eachSeries jobIds, async.apply(@_disableJob), (error) =>
        return callback error if error?
        @_removeJob pingJobId, callback

  _disableJob: (jobId, callback) =>
    @kue.Job.get jobId, (error, job) =>
      {sendTo, nodeId} = job.data

      @client.hset 'ping:disabled', "#{sendTo}:#{nodeId}", Date.now(), callback

  _removeJob: (jobId, callback) =>
    @kue.Job.get jobId, (error, job) =>
      job.remove() unless error?
      callback()

  isSystemStable: (callback) =>
    bucket1 = @_getBucket 2
    bucket2 = @_getBucket 3
    bucket3 = @_getBucket 4
    bucket4 = @_getBucket 5
    bucket5 = @_getBucket 6

    tasks = [
      async.apply @client.hmget, "ping:count:#{bucket1}", 'total:ping', 'total:pong'
      async.apply @client.hmget, "ping:count:#{bucket2}", 'total:ping', 'total:pong'
      async.apply @client.hmget, "ping:count:#{bucket3}", 'total:ping', 'total:pong'
      async.apply @client.hmget, "ping:count:#{bucket4}", 'total:ping', 'total:pong'
      async.apply @client.hmget, "ping:count:#{bucket5}", 'total:ping', 'total:pong'
    ]

    async.series tasks, (error, results) =>
      return callback error if error?
      stats = new Stats()
      undefinedPongs = _.any results, ([ping,pong]) => _.isUndefined(pong) || _.isNull(pong)
      return callback null, false if undefinedPongs

      zeroPongs = _.any results, ([ping,pong]) => parseInt(pong) == 0
      return callback null, false if zeroPongs

      debug 'stable results', results
      _.each results, ([ping,pong]) =>
        avg = parseInt(pong) / parseInt(ping)
        stats.push avg if pong?

      dev = stats.σ()
      callback null, dev == 0 || dev.toFixed(2) <= 0.01

  _getBucket: (modifier=0) =>
    _.floor (Date.now() - (@pingInterval*modifier)) / @pingInterval

  clearIfUnstable: (stable, callback) =>
    return callback() if stable
    @client.del 'ping:count:total', callback

  isIntervalAvailable: ({sendTo,nodeId}, callback) =>
    @client.smembers "interval/job/#{sendTo}/#{nodeId}", (error, jobIds) =>
      return callback error if error?
      async.detect jobIds, @findJob, (job) =>
        callback null, job?

  findJob: (jobId, callback) =>
    @kue.Job.get jobId, (ignoredError, job) =>
      callback job

module.exports = PingJobProcessor
