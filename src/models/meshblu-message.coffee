MeshbluHttp = require 'meshblu-http'
debug = require('debug')('interval-service')
_ = require 'lodash'

class MeshbluMessage
  constructor: (config) ->
    debug 'loading meshbluMessage with', config
    @meshbluHttp = new MeshbluHttp(config)

  message: (uuids, data) =>
    payload = _.merge {}, data, devices: uuids
    debug 'sending payload:', payload
    @meshbluHttp.message payload, =>
      debug 'payload sent:', payload

module.exports = MeshbluMessage
