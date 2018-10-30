const grpc = require('grpc')
const protoLoader = require('@grpc/proto-loader')
const protoPath = require('./config').server.protocol
const packageDefinition = protoLoader.loadSync(protoPath, {
  enums: String,
  defaults: true
})
const service = grpc.loadPackageDefinition(packageDefinition).maalfrid.service.aggregator.Aggregator.service
const {
  detectLanguages,
  generateAggregate,
  updateEntities,
  updateSeeds
} = require('./db')
const config = require('./config')
const log = config.logger

function runLanguageDetection (call, callback) {
  log.trace('runLanguageDetection', call)

  const detectAll = call.request['detect_all']

  detectLanguages({detectAll})
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

  const startTime = call.request['start_time']
  const endTime = call.request['end_time']

  generateAggregate({startTime, endTime})
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

  const name = call.request['name']
  const labels = call.request['labels']

  updateEntities({name, labels})
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

function getServer () {
  const server = new grpc.Server()

  server.addService(service, {
    runLanguageDetection,
    runAggregation,
    syncEntities
  })

  return server
}

module.exports = {
  getServer
}
