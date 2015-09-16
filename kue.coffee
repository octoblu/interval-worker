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

queue.process 'interval', process.env.INTERVAL_JOBS ? 100, (job, done) =>
  debug 'processing interval job', job.id, 'data', job.data
  redis.mget [ "active/#{job.data.groupId}", "active/#{job.data.targetId}" ],
    (err, active) =>
      debug 'active', err, active
      return done() if ! _.eq active, ['true','true']
      debug 'creating a new job!'
      meshbluMessage.message [job.data.targetId], timestamp: Date.now(), (err, res) =>
        return done(err) if err
        job =
          queue.create('interval', job.data).
          delay(job.data.intervalTime).
          removeOnComplete(true).
          attempts(process.env.INTERVAL_ATTEMPTS ? 999).
          ttl(process.env.INTERVAL_TTL ? 10000).
          save =>
            debug 'created a job', job.id
            redis.set "job/#{job.data.targetId}", job.id
            done()
