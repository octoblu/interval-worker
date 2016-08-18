_          = require 'lodash'
async      = require 'async'
debug      = require('debug')('nanocyte-interval-service:pong-job-processor')

class PongJobProcessor
  constructor: (options) ->
    {@client} = options

  processJob: (job, ignore, callback) =>
    debug 'processing pong job', job.id, 'data', JSON.stringify job.data
    { bucket, sendTo, nodeId, transactionId } = job.data
    return callback new Error 'no data' unless bucket?
    redisNodeId = transactionId ? nodeId
    flowNodeKey = "#{sendTo}:#{redisNodeId}"

    @client.hincrby "ping:count:#{bucket}", 'total:pong', 1, (error) =>
      return callback error if error?

      @client.hget "ping:count:total", flowNodeKey, (error, count) =>
        return callback error if error?
        count ?= 0
        if count > 1
          @client.del 'ping:count:total', callback
        else
          @client.hdel "ping:count:total", flowNodeKey, callback

module.exports = PongJobProcessor
