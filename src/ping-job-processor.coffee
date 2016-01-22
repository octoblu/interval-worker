_          = require 'lodash'
async      = require 'async'
debug      = require('debug')('nanocyte-interval-service:interval-job-processor')
cronParser = require 'cron-parser'
{Stats}    = require 'fast-stats'

class PingJobProcessor
  constructor: (options) ->
    {@meshbluMessage,@client,@kue,@pingInterval} = options

  processJob: (job, ignore, callback) =>
    debug 'processing interval job', job.id, 'data', JSON.stringify job.data
    {sendTo, nodeId} = job.data

    flowNodeKey = "#{sendTo}:#{nodeId}"
    bucket = @_getBucket()

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
              return @_removeJobs({pingJobId: job.id, sendTo, nodeId}, callback)

            message =
                topic: 'ping'
                payload:
                  from: nodeId

            tasks = [
              async.apply @client.hincrby, "ping:count:#{bucket}", 'total:ping', 1
              async.apply @meshbluMessage.message, [sendTo], message
            ]

            if systemStable
              tasks.push async.apply @client.hincrby, 'ping:count:total', flowNodeKey, 1

            async.series tasks, callback

  _removeJobs: ({pingJobId, sendTo, nodeId}, callback) =>
    @client.smembers "interval/job/#{sendTo}/#{nodeId}", (err, jobIds) =>
      jobIds ?= []
      jobIds.push pingJobId
      async.eachSeries jobIds, async.apply(@_removeJob), callback

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

      _.each results, ([ping,pong]) =>
        avg = parseInt(pong) / parseInt(ping)
        stats.push avg if pong?

      dev = stats.σ()
      callback null, dev == 0 || dev.toFixed(2) <= 0.01

  _getBucket: (modifier=1) =>
    _.floor Date.now()  / (@pingInterval * modifier)

  clearIfUnstable: (stable, callback) =>
    return callback() if stable
    @client.del 'ping:count:total', callback

module.exports = PingJobProcessor
