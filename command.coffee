Server        = require './src/server'

class Command
  constructor: ->
    @serverOptions =
      intervalTTL:        parseInt process.env.INTERVAL_TTL || 10000
      intervalJobs:       parseInt process.env.INTERVAL_JOBS || 10
      intervalAttempts:   parseInt process.env.INTERVAL_ATTEMPTS || 999
      intervalPromotion:  parseInt process.env.INTERVAL_PROMOTION || 50
      minTimeDiff:        parseInt process.env.MIN_TIME_DIFF || 500
      redisPort:          parseInt process.env.REDIS_PORT || 6379
      redisHost:          process.env.REDIS_HOST || 'localhost'

  panic: (error) =>
    console.error error.stack
    process.exit 1

  run: =>
    server = new Server @serverOptions
    server.run (error) =>
      return @panic error if error?

      {address,port} = server.address()
      console.log "Server listening on #{address}:#{port}"

command = new Command()
command.run()
