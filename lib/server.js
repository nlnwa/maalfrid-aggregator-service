const grpc = require('grpc')
const protoLoader = require('@grpc/proto-loader')
const protoPath = require('./config').server.protocol
const packageDefinition = protoLoader.loadSync(protoPath, {
  enums: String,
  defaults: true,
  longs: Number
})
/**
 * @type {{
 *   aggregator: {Aggregator: {service: *}}
 * }}
 */
const service = grpc.loadPackageDefinition(packageDefinition).maalfrid.service

const {
  detectLanguages,
  generateAggregate,
  syncSeedsAndEntities,
  generateStatistics,
  connect,
  disconnect
} = require('./db')

const config = require('./config')
const log = config.logger
const { grpcError } = require('./error')

function runLanguageDetection (call, callback) {
  log.trace('runLanguageDetection', call.request)
  const { detectAll } = call.request

  detectLanguages(detectAll)
    .then(() => callback(null, {}))
    .catch((error) => {
      log.error(error.message)
      callback(grpcError(error), callback)
    })
}

function runAggregation (call, callback) {
  log.trace('runAggregation', call.request)
  const { jobExecutionId } = call.request

  generateAggregate(jobExecutionId)
    .then(() => callback(null, {}))
    .catch((error) => {
      log.error(error.message)
      callback(grpcError(error), callback)
    })
}

function syncEntities (call, callback) {
  log.trace('syncEntities', call.request)
  const { labels } = call.request

  syncSeedsAndEntities(labels)
    .then(() => callback(null, {}))
    .catch((error) => {
      log.error(error.message)
      callback(grpcError(error), callback)
    })
}

function filterAggregate (call, callback) {
  log.trace('filterAggregate', call.request)
  const { jobExecutionId, seedId } = call.request

  generateStatistics(jobExecutionId, seedId)
    .then(() => callback(null, {}))
    .catch((error) => {
      log.error(error.message)
      callback(grpcError(error), callback)
    })
}

function getServer () {
  const server = new grpc.Server()

  server.addService(service.aggregator.Aggregator.service, {
    runLanguageDetection,
    runAggregation,
    syncEntities,
    filterAggregate
  })

  return server
}

/**
 * Register shutdown handler function
 *
 * @param handlerFn Signal handler function
 */
function handleShutdown (handlerFn) {
  const signals = ['SIGHUP', 'SIGINT', 'SIGQUIT', 'SIGABRT', 'SIGTERM']
  signals.forEach(signal => process.on(signal, (signal) => {
    log.info(`Received ${signal}, shutting down...`)
    handlerFn()
  }))
}

async function serve (host = config.server.host, port = config.server.port) {
  const server = getServer()
  const credentials = grpc.ServerCredentials.createInsecure()

  const boundPort = server.bind(host + ':' + port, credentials)
  if (boundPort !== port) {
    log.error('Failed to bind to port', port)
    process.exit(1)
  }

  try {
    // connect to database
    const connection = await connect()

    // register shutdown handler
    handleShutdown(() =>
      server.tryShutdown(() => disconnect(connection)
        .then(() => process.exit(0))
        .catch(() => process.exit(1))))
  } catch (error) {
    log.error(error.message)
    process.exit(1)
  }

  server.start()

  log.info(`Server listening on ${host}:${port}`)
}

module.exports = {
  serve
}
