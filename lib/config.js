const version = require('../package.json').version
const name = require('../package.json').name
const logger = require('./logger')(name)

module.exports = {
  app: { name, version },
  logger,
  server: {
    host: process.env.HOST || '0.0.0.0',
    port: parseInt(process.env.PORT, 10) || 3011,
    protocol: 'node_modules/maalfrid-api/maalfrid/service/aggregator/aggregator.proto'
  },
  languageService: {
    host: process.env.LANGUAGE_SERVICE_HOST || 'localhost',
    port: process.env.LANGUAGE_SERVICE_PORT || 8672,
    protocol: 'node_modules/maalfrid-api/maalfrid/service/language/ls.proto'
  },
  languageDetection: {
    concurrency: parseInt(process.env.LANGUAGE_DETECTION_CONCURRENCY, 10) || 10
  },
  rethinkdb: {
    servers: [
      {
        host: process.env.DB_HOST || 'localhost',
        port: parseInt(process.env.DB_PORT) || 28015
      }
    ],
    db: process.env.DB_NAME || 'maalfrid',
    user: process.env.DB_USER || 'admin',
    password: process.env.DB_PASSWORD || '',
    log: logger.info,
    silent: true
  }
}
