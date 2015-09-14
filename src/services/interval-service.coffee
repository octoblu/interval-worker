
_ = require 'lodash'
debug = require('debug')('interval-service')

class IntervalService
  constructor: (options={}) ->
    @subscriptionsByUuid = {}
    @subscriptionsByTime = {}
    @intervals = {}

  subscribe: (flowId, nodeId, intervalTime) =>
    debug 'subscribing with flowId', flowId, 'nodeId', nodeId, 'intervalTime', intervalTime
    @subscriptionsByUuid[flowId] = intervalTime
    @subscriptionsByTime[intervalTime] ?= {}
    @subscriptionsByTime[intervalTime][flowId] = true
    if !@intervals[intervalTime]
      @intervals[intervalTime] = setInterval =>
        debug 'firing interval', intervalTime
        _.each @subscriptionsByTime[intervalTime], (key, uuid)=>
          debug ' - in interval', intervalTime, 'for', uuid
      , intervalTime

  unsubscribe: (flowId, nodeId) =>
    intervalTime = @subscriptionsByUuid[flowId]
    delete @subscriptionsByUuid[flowId]
    delete @subscriptionsByTime[intervalTime][flowId]
    if _.size(@subscriptionsByTime[intervalTime]) == 0
      debug 'clearing the interval', intervalTime
      clearInterval @intervals[intervalTime]
      delete @intervals[intervalTime]

module.exports = IntervalService
