_ = require 'lodash'
debug = require('debug')('interval-service')

class IntervalService
  constructor: (options={}) ->
    {@messenger} = options
    @subscriptionsByUuid = {}
    @subscriptionsByTime = {}
    @intervals = {}

  subscribeNode: (flowId, nodeId, intervalTime) =>
    debug 'subscribing with flowId', flowId, 'nodeId', nodeId, 'intervalTime', intervalTime

    if @subscriptionsByUuid[flowId]?[nodeId]?
      @unsubscribeNode flowId, nodeId

    @subscriptionsByUuid[flowId] ?= {}
    @subscriptionsByUuid[flowId][nodeId] = intervalTime

    @subscriptionsByTime[intervalTime] ?= {}
    @subscriptionsByTime[intervalTime][nodeId] = true

    if !@intervals[intervalTime]
      @intervals[intervalTime] = setInterval =>
        nodeIds = _.keys @subscriptionsByTime[intervalTime]
        debug 'firing interval', intervalTime, 'for', flowId, '/', nodeIds
        @messenger.message nodeIds, timestamp: Date.now() if @messenger
      , intervalTime

  unsubscribeNode: (flowId, nodeId) =>
    debug 'unsubscribing node', flowId, nodeId

    intervalTime = @subscriptionsByUuid[flowId][nodeId]

    delete @subscriptionsByUuid[flowId][nodeId]
    if _.size(@subscriptionsByUuid[flowId]) == 0
      delete @subscriptionsByUuid[flowId]

    delete @subscriptionsByTime[intervalTime][nodeId]
    if _.size(@subscriptionsByTime[intervalTime]) == 0
      debug 'clearing the interval', intervalTime
      clearInterval @intervals[intervalTime]
      delete @intervals[intervalTime]

  unsubscribeFlow: (flowId) =>
    _.each _.keys(@subscriptionsByUuid[flowId]), (nodeId) =>
      @unsubscribeNode flowId, nodeId

module.exports = IntervalService
