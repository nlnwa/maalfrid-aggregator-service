const program = require('commander')

const config = require('./config')
const log = config.logger

const {
  connect,
  disconnect,
  detectLanguages,
  generateAggregate,
  syncSeeds,
  syncSeedsAndEntities,
  generateStatistics
} = require('./db')

const { serve } = require('./server')

program.version(config.app.version)
program.name(config.app.name)

program
  .command('detect')
  .description('detect languages')
  .option('-a, --all [detectAll]', 'detect languages of all')
  .action(async (cmd) => {
    const { detectAll } = cmd

    log.info('App:', config.app.version, 'Node:', process.version)

    try {
      const connection = await connect()

      const result = await detectLanguages(detectAll)
      log.info(JSON.stringify(result, null, 2))

      await disconnect(connection)
    } catch (error) {
      log.error(error.message)
      process.exit(1)
    }
  })

program
  .command('sync')
  .option('--labels [labels]', 'comma separated list of labels', (value) => value.split(','), [])
  .description('synchronize entities and seeds from veidemann')
  .action(async (cmd) => {
    const { labels } = cmd

    log.info('App:', config.app.version, 'Node:', process.version)

    try {
      const connection = await connect()

      const result = await syncSeedsAndEntities(labels)
      log.info(JSON.stringify(result, null, 2))

      await disconnect(connection)
    } catch (error) {
      log.error(error.message)
      process.exit(1)
    }
  })

program
  .command('sync-seeds')
  .option('--labels [labels]', 'comma separated list of labels', (value) => value.split(','), [])
  .description('synchronize entities and seeds from veidemann')
  .action(async (cmd) => {
    const { labels } = cmd

    log.info('App:', config.app.version, 'Node:', process.version)

    try {
      const connection = await connect()

      const result = await syncSeeds(labels)
      log.info(JSON.stringify(result, null, 2))

      await disconnect(connection)
    } catch (error) {
      log.error(error.message)
      process.exit(1)
    }
  })

program
  .command('filter')
  .description('detect languages')
  .option('--job-execution-id [jobExecutionId]', 'id of job execution to process')
  .option('--seed-id [seedId]', 'seed id')
  .action(async (cmd) => {
    const { jobExecutionId, seedId } = cmd

    log.info('App:', config.app.version, 'Node:', process.version)

    try {
      const connection = await connect()

      const result = await generateStatistics(jobExecutionId, seedId)
      log.info(JSON.stringify(result, null, 2))

      await disconnect(connection)
    } catch (error) {
      log.error(error.message)
      process.exit(1)
    }
  })

program
  .command('aggregate')
  .description('aggregate extracted texts from veidemann to maalfrid')
  .option('--job-execution-id [jobExecutionId]', 'id of job execution to process')
  .action(async (cmd) => {
    const { jobExecutionId } = cmd

    log.info('App:', config.app.version, 'Node:', process.version)

    try {
      const connection = await connect()

      const result = await generateAggregate(jobExecutionId)
      log.info(JSON.stringify(result, null, 2))

      await disconnect(connection)
    } catch (error) {
      log.error(error.message)
      process.exit(1)
    }
  })

program
  .command('serve')
  .description('start server')
  .option('-h, --host <host>', 'server hostname')
  .option('-p, --port <port>', 'server port')
  .action(async (cmd) => {
    const { host, port } = cmd

    log.info('App:', config.app.version, 'Node:', process.version)

    try {
      serve(host, port)
    } catch (error) {
      log.error(error.message)
      process.exit(1)
    }
  })

program.parse(process.argv)
