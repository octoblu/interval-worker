MeshbluHttp = require 'meshblu-http'
MeshbluConfig = require 'meshblu-config'
debug = require('debug')('nanocyte-interval-service:meshblu-message')
_ = require 'lodash'

class MeshbluMessage
  constructor: (config) ->
    meshbluConfig = new MeshbluConfig({}).toJSON()
    debug 'loading meshbluMessage with', JSON.stringify meshbluConfig
    @meshbluHttp = new MeshbluHttp meshbluConfig

  message: (uuids, data, callback = =>) =>
    payload = _.merge {}, data, devices: uuids
    debug 'sending payload:', JSON.stringify payload
    @meshbluHttp.message payload, (err, res) =>
      # debug 'payload sent:', payload, 'error', err, 'result', res
      callback err, res

module.exports = MeshbluMessage
