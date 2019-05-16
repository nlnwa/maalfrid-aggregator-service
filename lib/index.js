const program = require('commander')

const parse = require('date-fns/parse')
const isValid = require('date-fns/is_valid')

const config = require('./config')
const log = config.logger

const {
  connect,
  disconnect,
  detectLanguages,
  generateAggregate,
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
      await connect()

      const result = await detectLanguages(detectAll)
      log.info(JSON.stringify(result, null, 2))
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
      await connect()

      const result = await syncSeedsAndEntities(labels)
      log.info(result)

      await disconnect()
    } catch (error) {
      log.error(error.message)
      process.exit(1)
    }
  })

program
  .command('filter')
  .description('detect languages')
  .option('--start-time [startTime]', 'detect languages of all', parse)
  .option('--end-time [endTime]', '', parse)
  .option('--seed-id [seedId]', 'seed id')
  .action(async (cmd) => {
    const { startTime, endTime, seedId } = cmd

    log.info('App:', config.app.version, 'Node:', process.version)

    try {
      await connect()

      const result = await generateStatistics(isValid(startTime) && startTime, isValid(endTime) && endTime, seedId)
      log.info(JSON.stringify(result, null, 2))

      await disconnect()
    } catch (error) {
      log.error(error.message)
      process.exit(1)
    }
  })

program
  .command('aggregate')
  .description('aggregate extracted texts from veidemann to maalfrid')
  .option('--start-time [startTime]', 'detect languages of all', parse)
  .option('--end-time [endTime]', '', parse)
  .action(async (cmd) => {
    const { startTime, endTime } = cmd

    log.info('App:', config.app.version, 'Node:', process.version)

    try {
      await connect()

      const result = await generateAggregate(isValid(startTime) && startTime, isValid(endTime) && endTime)
      log.info(JSON.stringify(result, null, 2))

      await disconnect()
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
