class MessagesController
  constructor: (options={}) ->
    {@intervalService} = options

  subscribe: (req, res) =>
    @intervalService.subscribeNode req.params.flowId, req.params.nodeId, req.params.intervalTime ? 1000
    res.status(201).end()

  unsubscribe: (req, res) =>
    @intervalService.unsubscribeFlow req.params.flowId
    res.status(201).end()

module.exports = MessagesController
