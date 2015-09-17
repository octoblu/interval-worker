_ = require 'lodash'
kue = require 'kue'
morgan = require 'morgan'
express = require 'express'
errorHandler = require 'errorhandler'
meshbluHealthcheck = require 'express-meshblu-healthcheck'
meshbluMessage = new (require './src/models/meshblu-message') require './meshblu.json'
redis = new (require 'ioredis');
debug = require('debug')('interval-service')
cronParser = require 'cron-parser'

queue = kue.createQueue
  promotion:
    interval: process.env.INTERVAL_PROMOTION ? 100

queue.process 'interval', process.env.INTERVAL_JOBS ? 1000, (job, done) =>
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
      "interval/time/#{job.data.targetId}",
      "interval/cron/#{job.data.targetId}" ],

      (err, jobInfo) =>
        [ activeGroup, activeTarget, intervalTime, cronString ] = jobInfo
        debug 'job info', err, jobInfo, job.id
        return done() if !(activeGroup and activeTarget)
        debug 'creating a new job!'
        meshbluMessage.message [job.data.targetId], timestamp: Date.now()
        # , (err, res) =>
        #   return done(err) if err

        if (cronString)
          now = new Date()
          now.setSeconds(now.getSeconds() + 1)
          now.setMilliseconds(0)
          nextTime = (cronParser.parseExpression cronString, currentDate: now).next()
          intervalTime = nextTime.getTime() - Date.now()
          debug 'cron parser results:', intervalTime/1000, 's on date', nextTime.toString()
          redis.set "interval/time/#{job.data.targetId}", intervalTime

        debug 'creating a job to run in', intervalTime

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
