
_ = require 'lodash'
debug = require('debug')('interval-service')

class IntervalService
  constructor: (options={}) ->
    @subscriptionsByUuid = {}
    @subscriptionsByTime = {}
    @intervals = {}

  subscribe: (flowId, nodeId, intervalTime) =>
    debug 'subscribing with flowId', flowId, 'nodeId', nodeId, 'intervalTime', intervalTime

    if @subscriptionsByUuid[flowId]?[nodeId]
      @unsubscribeNode flowId, nodeId

    @subscriptionsByUuid[flowId] ?= {}
    @subscriptionsByUuid[flowId][nodeId] = intervalTime

    @subscriptionsByTime[intervalTime] ?= {}
    @subscriptionsByTime[intervalTime][flowId] ?= {}
    @subscriptionsByTime[intervalTime][flowId][nodeId] = true

    if !@intervals[intervalTime]
      @intervals[intervalTime] = setInterval =>
        debug 'firing interval', intervalTime
        _.each @subscriptionsByTime[intervalTime], (nodes, flowId) =>
          _.each nodes, (val, nodeId) =>
            debug ' - in interval', intervalTime, 'for', flowId, '/', nodeId
      , intervalTime

  unsubscribeNode: (flowId, nodeId) =>
    debug 'unsubscribing node', flowId, nodeId

    intervalTime = @subscriptionsByUuid[flowId][nodeId]

    delete @subscriptionsByUuid[flowId][nodeId]
    if _.size(@subscriptionsByUuid[flowId]) == 0
      delete @subscriptionsByUuid[flowId]

    delete @subscriptionsByTime[intervalTime][flowId][nodeId]
    if _.size(@subscriptionsByTime[intervalTime][flowId]) == 0
      delete @subscriptionsByTime[intervalTime][flowId]

    if _.size(@subscriptionsByTime[intervalTime]) == 0
      debug 'clearing the interval', intervalTime
      clearInterval @intervals[intervalTime]
      delete @intervals[intervalTime]

  unsubscribeFlow: (flowId) =>
    _.each @subscriptionsByUuid[flowId], (key, nodeId)=>
      @unsubscribeNode flowId, nodeId

module.exports = IntervalService
