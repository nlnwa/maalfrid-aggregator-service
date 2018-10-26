const grpc = require('grpc')
const protoLoader = require('@grpc/proto-loader')
const protoPath = require('./config').server.protocol
const packageDefinition = protoLoader.loadSync(protoPath, {
  enums: String,
  defaults: true
})
const service = grpc.loadPackageDefinition(packageDefinition).maalfrid.service.aggregator.Aggregator.service
const {
  detectAndUpdateTextsMissingCode,
  generateAggregate,
  updateEntities,
  updateSeeds
} = require('./db')
const config = require('./config')
const log = config.logger

function runLanguageDetection (call, callback) {
  log.trace('runLanguageDetection', call)
  detectAndUpdateTextsMissingCode()
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
  log.trace('runAggregation', call)
  generateAggregate()
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
  log.trace('syncEntities', call)
  updateEntities()
    .then(() => {
      callback(null, {})
    })
    .catch((error) => {
      error.code = grpc.status.INTERNAL
      callback(error, null)
      log.error(error)
    })
}

function syncSeeds (call, callback) {
  log.trace('syncSeeds', call)
  updateSeeds()
    .then(() => {
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
    syncSeeds
  })

  return server
}

module.exports = {
  getServer
}
