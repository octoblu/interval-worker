_           = require 'lodash'
async       = require 'async'
debug       = require('debug')('nanocyte-interval-service:ping-job-processor')
MeshbluHttp = require 'meshblu-http'
{Stats}     = require 'fast-stats'

class PingJobProcessor
  constructor: (options, dependencies={}) ->
    {
      @meshbluConfig
      @client
      @kue
      @pingInterval
      @queue
      @registerJobProcessor
    } = options
    @MeshbluHttp = dependencies.MeshbluHttp ? MeshbluHttp

  processJob: (job, ignore, callback) =>
    debug 'processing ping job', job.id, 'data', JSON.stringify job.data
    {sendTo, nodeId, transactionId} = job.data

    redisNodeId = transactionId ? nodeId

    flowNodeKey = "#{sendTo}:#{redisNodeId}"
    bucket = @_getBucket()

    @isIntervalAvailable {sendTo, nodeId, transactionId}, (error, intervalAvailable) =>
      return callback error if error?
      return callback() unless intervalAvailable

      @isSystemStable (error, systemStable) =>
        return callback error if error?

        debug 'isSystemStable?', systemStable

        @clearIfUnstable systemStable, (error) =>
          return callback error if error?

          @client.hget "ping:count:total", flowNodeKey, (error, count) =>
            return callback error if error?
            debug 'ping:count:total', flowNodeKey, count

            count ?= 0
            if systemStable && parseInt(count || 0) >= 5
              return @_disableJobs({pingJobId: job.id, sendTo, nodeId, transactionId}, callback)

            keys = [
              "interval/uuid/#{sendTo}/#{redisNodeId}"
              "interval/token/#{sendTo}/#{redisNodeId}"
            ]
            @client.mget keys, (error, result) =>
              return callback error if error?

              [uuid, token] = result

              config = _.defaults {uuid, token}, @meshbluConfig
              meshbluHttp = new @MeshbluHttp config

              message =
                  devices: [sendTo]
                  topic: 'ping'
                  payload:
                    from: nodeId
                    nodeId: nodeId
                    transactionId: transactionId
                    bucket: @_getBucket()
                    timestamp: _.now()

              tasks = [
                async.apply @client.hincrby, "ping:count:#{bucket}", 'total:ping', 1
                async.apply meshbluHttp.message, message
                async.apply @registerJobProcessor.createPingJob, job.data
              ]

              if systemStable
                tasks.push async.apply @client.hincrby, 'ping:count:total', flowNodeKey, 1

              async.series tasks, callback

  _disableJobs: ({pingJobId, sendTo, nodeId, transactionId}, callback) =>
    redisNodeId = transactionId ? nodeId
    @client.smembers "interval/job/#{sendTo}/#{redisNodeId}", (err, jobIds) =>
      jobIds ?= []
      async.eachSeries jobIds, async.apply(@_disableJob, {sendTo, nodeId, transactionId}), (error) =>
        return callback error if error?
        @_removeJob pingJobId, callback

  _disableJob: ({sendTo, nodeId, transactionId}, jobId, callback) =>
    redisNodeId = transactionId ? nodeId
    @client.hset 'ping:disabled', "#{sendTo}:#{redisNodeId}", Date.now(), callback

  _removeJob: (jobId, callback) =>
    return callback() unless jobId?
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
      undefinedPongs = _.some results, ([ping,pong]) => _.isUndefined(pong) || _.isNull(pong)
      return callback null, false if undefinedPongs

      zeroPongs = _.some results, ([ping,pong]) => parseInt(pong) == 0
      return callback null, false if zeroPongs

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

  isIntervalAvailable: ({sendTo,nodeId,transactionId}, callback) =>
    redisNodeId = transactionId ? nodeId
    @client.hexists 'ping:disabled', "#{sendTo}:#{redisNodeId}", (error, exists) =>
      return callback error if error?
      return callback null, false if exists == 1
      @client.exists "interval/active/#{sendTo}/#{redisNodeId}", (error, exists) =>
        return callback error if error?
        callback null, exists == 1

module.exports = PingJobProcessor
