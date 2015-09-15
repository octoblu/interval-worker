morgan = require 'morgan'
express = require 'express'
errorHandler = require 'errorhandler'
meshbluHealthcheck = require 'express-meshblu-healthcheck'
meshbluMessage = new (require './src/models/meshblu-message') require './meshblu.json'
intervalService = new (require './src/services/interval-service-single') {messenger:meshbluMessage}
messagesController = new (require './src/controllers/messages-controller') {intervalService:intervalService}

PORT  = process.env.PORT ? 80

app = express()
app.use morgan 'dev'
app.use errorHandler()
app.use meshbluHealthcheck()

app.post '/:flowId/:nodeId/:intervalTime', messagesController.subscribe
app.delete '/:flowId', messagesController.unsubscribe

server = app.listen PORT, ->
  host = server.address().address
  port = server.address().port

  console.log "Server running on #{host}:#{port}"
