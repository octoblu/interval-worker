async = require 'async'
intervals = require '../intervals.json'
redis = require 'redis'

pingInterval = parseInt process.env.PING_INTERVAL || (1000 * 60 * 60) # 1 hour
redisPort = parseInt process.env.REDIS_PORT || 6379
redisHost = process.env.REDIS_HOST || 'localhost'

client = redis.createClient redisPort, redisHost

async.each intervals, ({cronString, sendTo, nodeId, intervalTime}, next) =>
  return next() if cronString?
  return next() unless intervalTime?
  client.get "interval/time/#{sendTo}/#{nodeId}", (error, data) =>
    console.log data, intervalTime
    return next() if parseInt(data) == parseInt(intervalTime)
    console.log 'Processing: ', "#{sendTo}:#{nodeId}"
    client.set "interval/time/#{sendTo}/#{nodeId}", intervalTime, next
, =>
  console.log 'done!'
  process.exit 0
