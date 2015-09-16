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

  redis.srem "interval/job/#{job.data.targetId}", job.id
  redis.smembers "interval/job/#{job.data.targetId}", (err, jobIds) =>

    _.each jobIds, (jobId) =>
      debug 'checking jobId', jobId, 'to', job.id
      return if jobId == job.id
      debug 'removing stale jobId', jobId
      kue.Job.get jobId, (err, job) =>
        return if err
        job.remove()

    redis.mget [
      "interval/active/#{job.data.groupId}",
      "interval/active/#{job.data.targetId}",
      "interval/time/#{job.data.targetId}" ],

      (err, jobInfo) =>
        [ activeGroup, activeTarget, intervalTime ] = jobInfo
        debug 'job information', err, jobInfo, activeGroup, activeTarget, intervalTime
        return done() if !(activeGroup and activeTarget)
        debug 'creating a new job!'
        meshbluMessage.message [job.data.targetId], timestamp: Date.now()
        # , (err, res) =>
        #   return done(err) if err
        job =
          queue.create('interval', job.data).
          delay(intervalTime).
          removeOnComplete(true).
          attempts(process.env.INTERVAL_ATTEMPTS ? 999).
          ttl(process.env.INTERVAL_TTL ? 10000).
          save (err) =>
            debug 'created a job', job.id
            redis.sadd "interval/job/#{job.data.targetId}", job.id
            done(err)
