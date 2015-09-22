KueWorker = require './kue-worker'

class Runner
  constructor: ->

  start: =>
    @kueWorker = new KueWorker
    @kueWorker.start()

module.exports = Runner
