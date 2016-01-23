_          = require 'lodash'
async      = require 'async'
debug      = require('debug')('nanocyte-interval-service:pong-job-processor')
cronParser = require 'cron-parser'
{Stats}    = require 'fast-stats'

class PongJobProcessor
  constructor: (options) ->
    {@client} = options

  processJob: (job, ignore, callback) =>
    debug 'processing ping job', job.id, 'data', JSON.stringify job.data
    {bucket} = job.data

    return callback new Error 'no data' unless bucket?
    @client.hincrby "ping:count:#{bucket}", 'total:pong', 1, callback

module.exports = PongJobProcessor
