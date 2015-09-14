morgan = require 'morgan'
express = require 'express'
bodyParser = require 'body-parser'
errorHandler = require 'errorhandler'
meshbluHealthcheck = require 'express-meshblu-healthcheck'
intervalService = new (require './src/services/interval-service')()
messagesController = new (require './src/controllers/messages-controller') {intervalService:intervalService}

PORT  = process.env.PORT ? 80

app = express()
app.use morgan 'dev'
app.use errorHandler()
app.use meshbluHealthcheck()
app.use bodyParser.urlencoded limit: '1mb', extended : true
app.use bodyParser.json limit : '1mb'

app.post '/:flowId/:nodeId/subscribe', messagesController.subscribe
app.delete '/:flowId/:nodeId/unsubscribe', messagesController.unsubscribe

server = app.listen PORT, ->
  host = server.address().address
  port = server.address().port

  console.log "Server running on #{host}:#{port}"
