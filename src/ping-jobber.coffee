async = require 'async'
RegisterJobProcessor = require './register-job-processor'
intervals = require '../intervals.json'

pingInterval = parseInt process.env.PING_INTERVAL || (1000 * 60 * 60) # 1 hour
redisPort = parseInt process.env.REDIS_PORT || 6379
redisHost = process.env.REDIS_HOST || 'localhost'

registerJobProcessor = new RegisterJobProcessor {pingInterval, redisPort, redisHost}

async.each intervals, ({sendTo, nodeId}, next) =>
  console.log 'Processing: ', "#{sendTo}:#{nodeId}"
  registerJobProcessor.createPingJob {sendTo, nodeId}, next
, =>
  console.log 'done!'
  process.exit 0
