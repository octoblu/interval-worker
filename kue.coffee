kue = require 'kue'
morgan = require 'morgan'
express = require 'express'
errorHandler = require 'errorhandler'
meshbluHealthcheck = require 'express-meshblu-healthcheck'
meshbluMessage = new (require './src/models/meshblu-message') require './meshblu.json'
intervalService = new (require './src/services/interval-service-single') {messenger:meshbluMessage}
messagesController = new (require './src/controllers/messages-controller') {intervalService:intervalService}
debug = require('debug')('interval-service')

queue = kue.createQueue();

queue.process 'subscribe-node', (job, done) =>
  debug 'processing subscribe-node job', job
  messagesController.subscribe
    params:
      flowId: job.data.flowId
      nodeId: job.data.nodeId
      intervalTime: job.data.intervalTime
  done()

queue.process 'unsubscribe-flow', (job, done) =>
  debug 'processing unsubscribe-flow job', job
  messagesController.unsubscribe
    params:
      flowId: job.flowId
  done()
