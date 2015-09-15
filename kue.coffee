_ = require 'lodash'
kue = require 'kue'
morgan = require 'morgan'
express = require 'express'
errorHandler = require 'errorhandler'
meshbluHealthcheck = require 'express-meshblu-healthcheck'
meshbluMessage = new (require './src/models/meshblu-message') require './meshblu.json'
redis = new (require 'ioredis');
debug = require('debug')('interval-service')

queue = kue.createQueue();

queue.process 'interval', (job, done) =>
  debug 'processing interval job', job.id
  meshbluMessage.message [job.data.targetId], timestamp: Date.now()
  redis.mget(
    [ "repeat/#{job.data.targetId}", "repeat/#{job.data.groupId}" ],
    (err, repeat) =>
      debug 'repeat', err, repeat
      if _.eq repeat, ['true','true']
        debug 'creating a new job!'
        job =
          queue.create('interval', job.data).
          delay(job.data.intervalTime).
          removeOnComplete(true).
          save =>
            debug 'created a job', job.id
            redis.set "job/#{job.data.targetId}", job.id
      done()
  )
