_          = require 'lodash'
async      = require 'async'
debug      = require('debug')('nanocyte-interval-service:register-job-processor')
cronParser = require 'cron-parser'
{Stats}    = require 'fast-stats'

class RegisterJobProcessor
  constructor: (options) ->
    {@client,@kue,@queue,@pingInterval,@intervalAttempts,@intervalTTL,@minTimeDiff} = options

  processJob: (job, ignore, callback) =>
    debug 'processing register job', job.id, 'data', JSON.stringify job.data
    async.series [
      async.apply @createIntervalProperties, job.data
      async.apply @createIntervalJob, job.data
      async.apply @createPingJob, job.data
    ], callback

  createPingJob: (data, callback) =>
    {sendTo, nodeId} = data
    job = @queue.create('ping', data)
      .delay(@pingInterval)
      .removeOnComplete(true)
      .save (error) =>
        return callback error if error?
        @client.set "interval/ping/#{sendTo}/#{nodeId}", job.id, callback

  createIntervalJob: (data, callback) =>
    {cronString, sendTo, nodeId, intervalTime} = data
    if cronString?
      try
        intervalTime = @calculateNextCronInterval cronString
        @client.set "interval/time/#{sendTo}/#{nodeId}", intervalTime
      catch error
        console.error error
        return callback()

    data.intervalTime = intervalTime

    return callback new Error "invalid intervalTime: #{intervalTime}" unless intervalTime >= 1000

    job = @queue.create('interval', data)
      .delay(intervalTime)
      .removeOnComplete(true)
      .attempts(@intervalAttempts)
      .ttl(@intervalTTL)
      .save (error) =>
        return callback error if error?
        async.series [
          async.apply @client.del, "interval/job/#{sendTo}/#{nodeId}"
          async.apply @client.sadd, "interval/job/#{sendTo}/#{nodeId}", job.id
        ], callback

  createIntervalProperties: (data, callback) =>
    {sendTo, nodeId, intervalTime, cronString, nonce} = data
    @client.mset "interval/active/#{sendTo}/#{nodeId}", 'true',
      "interval/time/#{sendTo}/#{nodeId}", intervalTime || '',
      "interval/cron/#{sendTo}/#{nodeId}", cronString || '',
      "interval/nonce/#{sendTo}/#{nodeId}", nonce || ''
    , callback

  calculateNextCronInterval: (cronString, currentDate) =>
    currentDate ?= new Date
    timeDiff = 0
    parser = cronParser.parseExpression cronString, currentDate: currentDate

    while timeDiff <= @minTimeDiff
      nextDate = parser.next()
      nextDate.setMilliseconds 0
      timeDiff = nextDate - currentDate

    return timeDiff

module.exports = RegisterJobProcessor
