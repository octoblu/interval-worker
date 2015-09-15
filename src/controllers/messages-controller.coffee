debug = require('debug')('interval-service')

class MessagesController
  constructor: (options={}) ->
    {@intervalService} = options

  subscribe: (req, res) =>
    debug 'subscribe with req', req
    @intervalService.subscribeNode req.params.flowId, req.params.nodeId, req.params.intervalTime ? 1000
    res.status(201).end() if res

  unsubscribe: (req, res) =>
    debug 'unsubscribe with req', req
    @intervalService.unsubscribeFlow req.params.flowId
    res.status(201).end() if res

module.exports = MessagesController
