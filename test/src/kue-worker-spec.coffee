KueWorker = require '../../src/kue-worker'
debug = require('debug')('mocha-test')

describe 'KueWorker', ->
  beforeEach ->
    @kue = { Job:{} }
    @kue.createQueue = sinon.spy()
    @kue.Job.get = sinon.spy()
    dependencies = {}
    dependencies.kue = @kue
    dependencies.IORedis = IORedis
    dependencies.MeshbluMessage = MeshbluMessage
    @sut = new KueWorker dependencies

  describe '->processJob', ->
    describe 'when called with a job', ->
      beforeEach (done) ->
        @sut.getTargetJobs = sinon.stub().yields null, ['some-job']
        @sut.removeJobs = sinon.stub()
        @sut.getJobInfo = sinon.stub().yields null
        @job = data: targetId: 'some-job', groupId: 'some-group'
        @sut.processJob @job, (@error) => done()

      it 'should not have an error', ->
        expect(@error).to.not.exist

      it 'should call getTargetJobs', ->
        expect(@sut.getTargetJobs).to.have.been.calledWith @job

      it 'should call removeJobs', ->
        expect(@sut.removeJobs).to.have.been.calledWith ['some-job']

      it 'should call getJobInfo', ->
        expect(@sut.getJobInfo).to.have.been.calledWith @job

    describe 'when called with another job', ->
      beforeEach (done) ->
        @sut.getTargetJobs = sinon.stub().yields null, ['another-job']
        @sut.removeJobs = sinon.stub()
        @sut.getJobInfo = sinon.stub().yields null
        @job = data: targetId: 'another-job', groupId: 'another-group'
        @sut.processJob @job, (@error) => done()

      it 'should not have an error', ->
        expect(@error).to.not.exist

      it 'should call getTargetJobs', ->
        expect(@sut.getTargetJobs).to.have.been.calledWith @job

      it 'should call removeJobs', ->
        expect(@sut.removeJobs).to.have.been.calledWith ['another-job']

      it 'should call getJobInfo', ->
        expect(@sut.getJobInfo).to.have.been.calledWith @job

  describe '->getTargetJobs', ->
    describe 'when called with a job', ->
      beforeEach (done) ->
        @sut.redis.srem = sinon.spy()
        @sut.redis.smembers = sinon.stub().yields null, ['some-job']
        job = data: targetId: 'some-job'
        @sut.getTargetJobs job, (@error) => done()

      it 'should not have an error', ->
        expect(@error).to.not.exist

      it 'should call redis.srem', ->
        expect(@sut.redis.srem).to.have.been.calledWith "interval/job/some-job"

      it 'should call redis.smembers', ->
        expect(@sut.redis.smembers).to.have.been.calledWith "interval/job/some-job"

    describe 'when called with another job', ->
      beforeEach (done) ->
        @sut.removeJobs = sinon.spy()
        @sut.redis.srem = sinon.spy()
        @sut.redis.smembers = sinon.stub().yields null, ['another-job']
        job = data: targetId: 'another-job'
        @sut.getTargetJobs job, (@error) => done()

      it 'should not have an error', ->
        expect(@error).to.not.exist

      it 'should call redis.srem', ->
        expect(@sut.redis.srem).to.have.been.calledWith "interval/job/another-job"

      it 'should call redis.smembers', ->
        expect(@sut.redis.smembers).to.have.been.calledWith "interval/job/another-job"

  describe '->removeJob', ->
    describe 'when called with a job', ->
      beforeEach (done) ->
        @kue.Job.get = sinon.stub().yields null, remove: sinon.spy()
        @sut.removeJob 'some-job', (@error) => done()

      it 'should call kue.get with the job ids', ->
        expect(@kue.Job.get).to.have.been.calledWith 'some-job'

    describe 'when called with another job', ->
      beforeEach (done) ->
        @kue.Job.get = sinon.stub().yields null, remove: sinon.spy()
        @sut.removeJob 'another-job', (@error) => done()

      it 'should call kue.get with the job ids', ->
        expect(@kue.Job.get).to.have.been.calledWith 'another-job'

  describe '->getJobInfo', ->
    describe 'when called with a job', ->
      beforeEach (done) ->
        job = data: targetId: 'some-job', groupId: 'some-group'
        jobInfo = [{id:'activeGroup'}, {id:'activeTarget'}, {id: 'intervalTime'}, {id: 'cronString'}]
        @sut.redis.mget = sinon.stub().yields null, jobInfo
        @sut.getJobInfo job, (@error, @jobInfo) => done()

      it 'should not have an error', ->
        expect(@error).to.not.exist

      it 'should call redis.mget to be called with keys', ->
        keys = [
          "interval/active/some-group",
          "interval/active/some-job",
          "interval/time/some-job",
          "interval/cron/some-job"
        ]
        expect(@sut.redis.mget).to.be.calledWith keys

      it 'should yield jobInfo', ->
        jobInfo = [{id:'activeGroup'}, {id:'activeTarget'}, {id: 'intervalTime'}, {id: 'cronString'}]
        expect(@jobInfo).to.deep.equal jobInfo

    describe 'when called with another job', ->
      beforeEach (done) ->
        job = data: targetId: 'another-job', groupId: 'another-group'
        jobInfo = [{id:'activeGroup'}, {id:'activeTarget'}, {id: 'intervalTime'}, {id: 'cronString'}]
        @sut.redis.mget = sinon.stub().yields null, jobInfo
        @sut.getJobInfo job, (@error, @jobInfo) => done()

      it 'should not have an error', ->
        expect(@error).to.not.exist

      it 'should call redis.mget to be called with keys', ->
        keys = [
          "interval/active/another-group",
          "interval/active/another-job",
          "interval/time/another-job",
          "interval/cron/another-job"
        ]
        expect(@sut.redis.mget).to.be.calledWith keys

      it 'should yield jobInfo', ->
        jobInfo = [{id:'activeGroup'}, {id:'activeTarget'}, {id: 'intervalTime'}, {id: 'cronString'}]
        expect(@jobInfo).to.deep.equal jobInfo

  describe '->calculateNextCronInterval', ->
    fakeDate = new Date(2015, 0, 1, 15, 0, 0, 0)
    now = new Date

    describe 'when called with seconds option and real date', ->
      it 'should result in a next time of at most 1000 ms', ->
        result = @sut.calculateNextCronInterval "* * * * * *", now
        nextDate = new Date(now.getTime() + result)
        debug 'result from', now, 'to', nextDate.toString(), result, 'ms'
        nextSecond = nextDate.getSeconds()- now.getSeconds()
        if nextSecond == -59
          nextSecond = 1
        expect(nextSecond).to.equal 1
        expect(nextDate.getMilliseconds()).to.equal 0
        expect(result).to.be.at.most(1000)

    describe 'when called with every 15 seconds option and real date', ->
      it 'should result in a next time of at most 15000 ms', ->
        result = @sut.calculateNextCronInterval "*/15 * * * * *", now
        nextDate = new Date(now.getTime() + result)
        debug 'result from', now, 'to', nextDate.toString(), result, 'ms'
        expect(nextDate.getSeconds() % 15).to.equal 0
        expect(nextDate.getMilliseconds()).to.equal 0
        expect(result).to.be.at.most(15000)

    describe 'when called with minutes option and real date', ->
      it 'should result in a next time of at most 60000 ms', ->
        result = @sut.calculateNextCronInterval "* * * * *", now
        nextDate = new Date(now.getTime() + result)
        debug 'result from', now, 'to', nextDate.toString(), result, 'ms'
        nextMinute = nextDate.getMinutes()- now.getMinutes()
        if nextMinute == -59
          nextMinute = 1
        expect(nextMinute).to.equal 1
        expect(nextDate.getSeconds()).to.equal 0
        expect(nextDate.getMilliseconds()).to.equal 0
        expect(result).to.be.at.most(60000)

    describe 'when called with every 15 minutes option and real date', ->
      it 'should result in a next time of at most 900000 ms', ->
        result = @sut.calculateNextCronInterval "*/15 * * * *", now
        nextDate = new Date(now.getTime() + result)
        debug 'result from', now, 'to', nextDate.toString(), result, 'ms'
        expect(nextDate.getMinutes() % 15).to.equal 0
        expect(nextDate.getSeconds()).to.equal 0
        expect(nextDate.getMilliseconds()).to.equal 0
        expect(result).to.be.at.most(900000)

    describe 'when called with seconds option and fake date', ->
      it 'should result in a next time of 1000 ms', ->
        result = @sut.calculateNextCronInterval "* * * * * *", fakeDate
        nextDate = new Date(fakeDate.getTime() + result)
        debug 'result from', fakeDate, 'to', new Date(fakeDate.getTime() + result).toString(), result, 'ms'
        nextSecond = nextDate.getSeconds()- fakeDate.getSeconds()
        if nextSecond == -59
          nextSecond = 1
        expect(nextSecond).to.equal 1
        expect(nextDate.getMilliseconds()).to.equal 0
        expect(result).to.equals(1000)

    describe 'when called with every 15 seconds option and fake date', ->
      it 'should result in a next time of 15000 ms', ->
        result = @sut.calculateNextCronInterval "*/15 * * * * *", fakeDate
        nextDate = new Date(fakeDate.getTime() + result)
        debug 'result from', fakeDate, 'to', new Date(fakeDate.getTime() + result).toString(), result, 'ms'
        expect(nextDate.getSeconds() % 15).to.equal 0
        expect(nextDate.getMilliseconds()).to.equal 0
        expect(result).to.equals(15000)

    describe 'when called with minutes option and fake date', ->
      it 'should result in a next time of 60000 ms', ->
        result = @sut.calculateNextCronInterval "* * * * *", fakeDate
        nextDate = new Date(fakeDate.getTime() + result)
        debug 'result from', fakeDate, 'to', new Date(fakeDate.getTime() + result).toString(), result, 'ms'
        nextMinute = nextDate.getMinutes()- fakeDate.getMinutes()
        if nextMinute == -59
          nextMinute = 1
        expect(nextMinute).to.equal 1
        expect(nextDate.getSeconds()).to.equal 0
        expect(result).to.equals(60000)

    describe 'when called with every 15 minutes option and fake date', ->
      it 'should result in a next time of 900000 ms', ->
        result = @sut.calculateNextCronInterval "*/15 * * * *", fakeDate
        nextDate = new Date(fakeDate.getTime() + result)
        debug 'result from', fakeDate, 'to', new Date(fakeDate.getTime() + result).toString(), result, 'ms'
        expect(nextDate.getMinutes() % 15).to.equal 0
        expect(nextDate.getSeconds()).to.equal 0
        expect(nextDate.getMilliseconds()).to.equal 0
        expect(result).to.equals(900000)

    describe 'when called with an invalid cron string', ->
      it 'should throw an error', ->
        hasError = false
        try
          @sut.calculateNextCronInterval "*/1000E * * * * *", fakeDate
        catch error
          hasError = true
        expect(hasError).to.equals true

class IORedis
  srem: =>
  smembers: =>
  mget: =>

class MeshbluMessage
  message: =>
