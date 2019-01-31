const config = require('./config')
const log = config.logger

const grpc = require('grpc')
const { getServer } = require('./server')

const server = getServer()
const credentials = grpc.ServerCredentials.createInsecure()

/**
 * Register event listener for signals
 *
 * @param {...string} signals - signals to listen for
 */
function handleSignals (...signals) {
  signals.forEach(signal => process.on(signal, () => {
    log.info('Received signal', signal, '- shutting down')
    server.tryShutdown(() => {
      log.info('Shutdown complete')
      process.exit(0)
    })
  }))
}

handleSignals('SIGHUP', 'SIGINT', 'SIGQUIT', 'SIGABRT', 'SIGTERM')

log.info('App:', config.app.version, 'Node:', process.version)

const boundPort = server.bind(config.server.host + ':' + config.server.port, credentials)
if (boundPort !== config.server.port) {
  log.error('Failed to bind to port', config.server.port)
  process.exit(1)
}

server.start()

log.info(`Listening on ${config.server.host}:${config.server.port}`)
