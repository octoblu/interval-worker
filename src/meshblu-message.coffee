MeshbluHttp = require 'meshblu-http'
MeshbluConfig = require 'meshblu-config'
debug = require('debug')('nanocyte-interval-service:meshblu-message')
_ = require 'lodash'

class MeshbluMessage
  constructor: (config) ->
    meshbluConfig = new MeshbluConfig({}).toJSON()
    debug 'loading meshbluMessage with', JSON.stringify meshbluConfig
    @meshbluHttp = new MeshbluHttp meshbluConfig

  stringifyError: (err) ->
    JSON.stringify err, ["message", "arguments", "type", "name"]

  message: (uuids, data, callback=->) =>
    payload = _.merge {}, data, devices: uuids
    debug 'sending payload:', JSON.stringify payload
    @meshbluHttp.message payload, (err, res) =>
      debug 'meshbluHttp error:', @stringifyError err if err
      callback err, res

module.exports = MeshbluMessage
