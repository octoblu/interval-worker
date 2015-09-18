KueWorker = require './kue-worker'

class Runner
  constructor: ->

  start: =>
    @kueWorker = new KueWorker

module.exports = Runner
