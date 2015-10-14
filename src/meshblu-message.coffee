MeshbluHttp = require 'meshblu-http'
MeshbluConfig = require 'meshblu-config'
debug = require('debug')('nanocyte-interval-service:meshblu-message')
_ = require 'lodash'

class MeshbluMessage
  constructor: (config) ->
    meshbluConfig = new MeshbluConfig({}).toJSON()
    @meshbluHttp = new MeshbluHttp meshbluConfig

  stringifyError: (err) ->
    JSON.stringify err, ["message", "arguments", "type", "name"]

  message: (uuids, data, callback=->) =>
    payload = _.merge {}, data, devices: uuids
    @meshbluHttp.message payload, (err, res) =>
      callback err, res

module.exports = MeshbluMessage
