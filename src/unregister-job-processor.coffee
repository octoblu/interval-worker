_          = require 'lodash'
async      = require 'async'
debug      = require('debug')('nanocyte-interval-service:unregister-job-processor')

class UnregisterJobProcessor
  constructor: (options) ->
    {@registerJobProcessor,@client,@kue,@queue} = options

  processJob: (job, ignore, callback) =>
    debug 'processing unregister job', job.id, 'data', JSON.stringify job.data
    {sendTo, nodeId, nonce} = job.data
    @client.get "interval/nonce/#{sendTo}/#{nodeId}", (error, savedNonce) =>
      return callback error if error?
      return callback new Error 'nonce does not match' unless savedNonce == nonce
      @registerJobProcessor.doUnregister {sendTo, nodeId}, callback

module.exports = UnregisterJobProcessor
