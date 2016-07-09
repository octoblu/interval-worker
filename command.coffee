_              = require 'lodash'
IntervalWorker = require './src/interval-worker'

class Command
  constructor: ->
    @options =
      intervalTTL:        parseInt process.env.INTERVAL_TTL || 10000
      intervalJobs:       parseInt process.env.INTERVAL_JOBS || 10
      intervalAttempts:   parseInt process.env.INTERVAL_ATTEMPTS || 999
      intervalPromotion:  parseInt process.env.INTERVAL_PROMOTION || 50
      minTimeDiff:        parseInt process.env.MIN_TIME_DIFF || 500
      redisUri:           process.env.REDIS_URI
      pingInterval:       parseInt process.env.PING_INTERVAL || (1000 * 60 * 60) # 1 hour

  panic: (error) =>
    console.error error.stack
    process.exit 1

  run: =>
    @panic new Error('Missing required environment variable: REDIS_URI') if _.isEmpty @options.redisUri

    server = new IntervalWorker @options
    server.run (error) =>
      return @panic error if error?

    process.on 'SIGTERM', =>
      console.log 'SIGTERM caught, exiting'
      process.exit 0

command = new Command()
command.run()
