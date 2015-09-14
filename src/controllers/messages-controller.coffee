class MessagesController
  constructor: (options={}) ->
    {@intervalService} = options

  subscribe: (req, res) =>
    @intervalService.subscribe req.params.flowId, req.params.nodeId, req.body.interval || 1000
    res.status(201).end()

  unsubscribe: (req, res) =>
    @intervalService.unsubscribe req.params.flowId, req.params.nodeId
    res.status(201).end()

module.exports = MessagesController
