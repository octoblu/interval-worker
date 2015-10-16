MeshbluHttp = require 'meshblu-http'
MeshbluConfig = require 'meshblu-config'
debug = require('debug')('nanocyte-interval-service:meshblu-message')
_ = require 'lodash'

class MeshbluMessage
  constructor: (config) ->
    meshbluConfig = new MeshbluConfig({}).toJSON()
    @meshbluHttp = new MeshbluHttp meshbluConfig

  message: (uuids, data, callback=->) =>
    payload = _.merge {}, data, devices: uuids
    @meshbluHttp.message payload, (error, result) =>
      debug 'meshblu-error:', error if error?
      callback error, result

module.exports = MeshbluMessage
