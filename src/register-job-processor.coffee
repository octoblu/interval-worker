_          = require 'lodash'
async      = require 'async'
debug      = require('debug')('nanocyte-interval-service:register-job-processor')
cronParser = require 'cron-parser'
Redlock    = require 'redlock'

class RegisterJobProcessor
  constructor: (options) ->
    {@client,@kue,@queue,@pingInterval,@intervalAttempts,@intervalTTL,@minTimeDiff} = options
    @redlock = new Redlock [@client], retryCount: 20, retryDelay: 100

  processJob: (job, ignore, callback) =>
    debug 'processing register job', job.id, 'data', JSON.stringify job.data
    {nodeId, sendTo, transactionId} = job.data
    redisNodeId = transactionId ? nodeId
    key = "#{sendTo}/#{redisNodeId}"
    @redlock.lock key, 5000, (error, lock) =>
      return callback error if error?

      async.series [
        async.apply @doUnregister, job.data
        async.apply @removeDisabledKey, job.data
        async.apply @createIntervalProperties, job.data
        async.apply @createPingJob, job.data
        async.apply @createIntervalJob, job.data
      ], (error) =>
        lock.unlock()
        callback error

  createPingJob: (data, callback) =>
    {sendTo, nodeId, transactionId, fireOnce} = data
    return callback() if fireOnce
    redisNodeId = transactionId ? nodeId
    job = @queue.create('ping', data)
      .ttl(5000)
      .events(false)
      .delay(@pingInterval)
      .removeOnComplete(true)
      .save (error) =>
        return callback error if error?
        @client.set "interval/ping/#{sendTo}/#{redisNodeId}", job.id, callback

  updateCronIntervalTime: ({cronString, sendTo, nodeId, transactionId}, callback) =>
    return callback() if _.isEmpty cronString
    redisNodeId = transactionId ? nodeId
    try
      intervalTime = @calculateNextCronInterval cronString
    catch error
      console.error 'calculateNextCronInterval', error
      return callback() if error?

    @client.set "interval/time/#{sendTo}/#{redisNodeId}", intervalTime, (error) =>
      return callback error if error?
      callback null, intervalTime

  createIntervalJob: (data, callback) =>
    {cronString, sendTo, nodeId, transactionId, intervalTime} = data
    redisNodeId = transactionId ? nodeId
    @updateCronIntervalTime {cronString, sendTo, nodeId, transactionId}, (error, cronIntervalTime) =>
      return callback error if error?
      intervalTime = cronIntervalTime if cronIntervalTime?
      data.intervalTime = intervalTime
      if intervalTime < @minTimeDiff
        console.error new Error "invalid intervalTime: #{intervalTime}"
        console.error {data}
        return callback()

      job = @queue.create('interval', data)
        .events(false)
        .delay(intervalTime)
        .removeOnComplete(true)
        .attempts(@intervalAttempts)
        .ttl(@intervalTTL)
        .save (error) =>
          return callback error if error?
          async.series [
            async.apply @client.del, "interval/job/#{sendTo}/#{redisNodeId}"
            async.apply @client.sadd, "interval/job/#{sendTo}/#{redisNodeId}", job.id
          ], callback

  createIntervalProperties: (data, callback) =>
    {sendTo, nodeId, transactionId, intervalTime, cronString, nonce} = data
    redisNodeId = transactionId ? nodeId
    @client.mset "interval/active/#{sendTo}/#{redisNodeId}", 'true',
      "interval/origTime/#{sendTo}/#{redisNodeId}", intervalTime || '',
      "interval/time/#{sendTo}/#{redisNodeId}", intervalTime || '',
      "interval/cron/#{sendTo}/#{redisNodeId}", cronString || '',
      "interval/nonce/#{sendTo}/#{redisNodeId}", nonce || ''
    , callback

  calculateNextCronInterval: (cronString, currentDate) =>
    currentDate ?= new Date
    timeDiff = 0
    parser = cronParser.parseExpression cronString, currentDate: currentDate
    while timeDiff <= @minTimeDiff
      nextDate = parser.next()?.toDate()
      if nextDate?
        nextDate.setMilliseconds 0
        timeDiff = nextDate - currentDate

    return timeDiff

  removeDisabledKey: ({sendTo, nodeId, transactionId}, callback) =>
    redisNodeId = transactionId ? nodeId
    @client.hdel 'ping:disabled', "#{sendTo}:#{redisNodeId}", callback

  doUnregister: ({sendTo, nodeId, transactionId}, callback) =>
    async.series [
      async.apply @removeIntervalProperties, {sendTo, nodeId, transactionId}
      async.apply @removeIntervalJobs, {sendTo, nodeId, transactionId}
      async.apply @removePingJob, {sendTo, nodeId, transactionId}
    ], callback

  removeIntervalProperties: ({sendTo, nodeId, transactionId}, callback) =>
    redisNodeId = transactionId ? nodeId
    @client.del "interval/active/#{sendTo}/#{redisNodeId}",
    "interval/time/#{sendTo}/#{redisNodeId}",
    "interval/cron/#{sendTo}/#{redisNodeId}",
    "interval/nonce/#{sendTo}/#{redisNodeId}", callback

  removeIntervalJobs: ({sendTo, nodeId, transactionId}, callback) =>
    redisNodeId = transactionId ? nodeId
    @client.smembers "interval/job/#{sendTo}/#{redisNodeId}", (error, jobIds) =>
      return callback error if error?
      async.each jobIds, @removeJob, callback

  removePingJob: ({sendTo, nodeId, transactionId}, callback) =>
    redisNodeId = transactionId ? nodeId
    @client.get "interval/ping/#{sendTo}/#{redisNodeId}", (error, jobId) =>
      return callback error if error?
      @removeJob jobId, callback

  removeJob: (jobId, callback) =>
    return callback() unless jobId?
    @kue.Job.get jobId, (error, job) =>
      job.remove() unless error?
      callback()

module.exports = RegisterJobProcessor
