MeshbluHttp = require 'meshblu-http'
debug = require('debug')('interval-service')
_ = require 'lodash'

class MeshbluMessage
  constructor: (config) ->
    debug 'loading meshbluMessage with', config
    @meshbluHttp = new MeshbluHttp(config)

  message: (uuids, data, callback = =>) =>
    payload = _.merge {}, data, devices: uuids
    debug 'sending payload:', payload
    @meshbluHttp.message payload, (err, res) =>
      debug 'payload sent:', payload, 'error', err, 'result', res
      callback err, res

module.exports = MeshbluMessage
