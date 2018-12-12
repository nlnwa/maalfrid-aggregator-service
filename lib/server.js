const grpc = require('grpc')
const protoLoader = require('@grpc/proto-loader')
const protoPath = require('./config').server.protocol
const packageDefinition = protoLoader.loadSync(protoPath, {
  enums: String,
  defaults: true,
  longs: Number
})
const service = grpc.loadPackageDefinition(packageDefinition).maalfrid.service.aggregator.Aggregator.service

const {
  detectLanguages,
  generateAggregate,
  updateEntities,
  updateSeeds,
  processAggregate
} = require('./db')

const { timestampToDate } = require('./util')
const config = require('./config')
const log = config.logger

function runLanguageDetection (call, callback) {
  log.trace('runLanguageDetection', call.request)
  const { detectAll } = call.request

  detectLanguages(detectAll)
    .then(() => {
      callback(null, {})
    })
    .catch((error) => {
      error.code = grpc.status.INTERNAL
      callback(error, null)
      log.error(error)
    })
}

function runAggregation (call, callback) {
  log.trace('runAggregation', call.request)
  const { startTime, endTime } = call.request

  generateAggregate(startTime && timestampToDate(startTime), endTime && timestampToDate(endTime))
    .then(() => {
      callback(null, {})
    })
    .catch((error) => {
      error.code = grpc.status.INTERNAL
      callback(error, null)
      log.error(error)
    })
}

function syncEntities (call, callback) {
  log.trace('syncEntities', call.request)
  const { name, labels } = call.request

  updateEntities(name, labels)
    .then(() => updateSeeds())
    .then(() => {
      callback(null, {})
    })
    .catch((error) => {
      error.code = grpc.status.INTERNAL
      callback(error, null)
      log.error(error)
    })
}

function filterAggregate (call, callback) {
  log.trace('filter', call.request)
  const { startTime, endTime, seedId } = call.request

  processAggregate(startTime && timestampToDate(startTime), endTime && timestampToDate(endTime), seedId)
    .then((_) => {
      callback(null, {})
    })
    .catch((error) => {
      error.code = grpc.status.INTERNAL
      callback(error, null)
      log.error(error)
    })
}

function getServer () {
  const server = new grpc.Server()

  server.addService(service, {
    runLanguageDetection,
    runAggregation,
    syncEntities,
    filterAggregate
  })

  return server
}

module.exports = {
  getServer
}
