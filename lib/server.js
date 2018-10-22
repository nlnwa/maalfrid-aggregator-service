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

function runLanguageDetection (call, callback) {
  detectAndUpdateTextsMissingCode()
    .then(() => {
      callback(null, {})
    })
    .catch((error) => {
      error.code = grpc.status.INTERNAL
      callback()
    })
}

function runAggregation (call, callback) {
  generateAggregate()
    .then(() => {
      callback(null, {})
    })
    .catch((error) => {
      error.code = grpc.status.INTERNAL
      callback(error, null)
    })
}

function syncEntities (call, callback) {
  updateEntities()
    .then(() => {
      callback(null, {})
    })
    .catch((error) => {
      error.code = grpc.status.INTERNAL
      callback(error, null)
    })
}

function syncSeeds (call, callback) {
  updateSeeds()
    .then(() => {
      callback(null, {})
    })
    .catch((error) => {
      error.code = grpc.status.INTERNAL
      callback(error, null)
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
