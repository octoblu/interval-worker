KueWorker = require './src/kue-worker'
morgan = require 'morgan'
express = require 'express'
errorHandler = require 'errorhandler'
meshbluHealthcheck = require 'express-meshblu-healthcheck'

PORT = process.env.PORT ? 80

app = express()
app.use morgan 'dev'
app.use errorHandler()
app.use meshbluHealthcheck()

server = app.listen PORT, ->
  host = server.address().address
  port = server.address().port

  new KueWorker().start()

  console.log "Server running on #{host}:#{port}"
