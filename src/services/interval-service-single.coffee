_ = require 'lodash'
debug = require('debug')('interval-service')

class IntervalServiceSingle
  constructor: (options={}) ->
    {@messenger} = options
    @subscriptionsByUuid = {}
    @intervals = {}

  subscribeNode: (flowId, nodeId, intervalTime) =>
    debug 'subscribing with flowId', flowId, 'nodeId', nodeId, 'intervalTime', intervalTime

    if @subscriptionsByUuid[flowId]?[nodeId]?
      @unsubscribeNode flowId, nodeId

    @subscriptionsByUuid[flowId] ?= {}
    @subscriptionsByUuid[flowId][nodeId] = true

    if !@intervals[nodeId]
      @intervals[nodeId] = setInterval =>
        debug 'firing interval', intervalTime, 'for', flowId, '/', nodeId
        @messenger.message [nodeId], timestamp: Date.now() if @messenger
      , intervalTime

  unsubscribeNode: (flowId, nodeId) =>
    debug 'unsubscribing node', flowId, nodeId

    delete @subscriptionsByUuid[flowId][nodeId]
    if _.size(@subscriptionsByUuid[flowId]) == 0
      delete @subscriptionsByUuid[flowId]

    clearInterval @intervals[nodeId]
    delete @intervals[nodeId]

  unsubscribeFlow: (flowId) =>
    _.each _.keys(@subscriptionsByUuid[flowId]), (nodeId) =>
      @unsubscribeNode flowId, nodeId

module.exports = IntervalServiceSingle
