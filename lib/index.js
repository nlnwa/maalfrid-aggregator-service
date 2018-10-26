const config = require('./config')
const log = config.logger

log.info('Version:', config.app.version, 'Node:', process.version)

const grpc = require('grpc')
const {getServer} = require('./server')

const server = getServer()
const credentials = grpc.ServerCredentials.createInsecure()

const boundPort = server.bind(config.server.host + ':' + config.server.port, credentials)
if (boundPort !== config.server.port) {
  log.error('Failed to bind to port', config.server.port)
  process.exit(1)
}

registerSignalHandler(signalHandler)

server.start()

log.info(`Listening on ${config.server.host}:${config.server.port}`)

function signalHandler (name) {
  return () => {
    log.info('Received signal', name, '- shutting down')
    server.tryShutdown(() => {
      log.info('Shutdown complete')
      process.exit(0)
    })
  }
}

function registerSignalHandler (handler) {
  process.on('SIGHUP', handler('SIGHUP'))
  process.on('SIGINT', handler('SIGINT'))
  process.on('SIGQUIT', handler('SIGQUIT'))
  process.on('SIGABRT', handler('SIGABRT'))
  process.on('SIGTERM', handler('SIGTERM'))
}
