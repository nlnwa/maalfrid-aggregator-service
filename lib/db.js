const dbConfig = require('./config').rethinkdb
const r = require('rethinkdb-js')(dbConfig)
const config = require('./config')
const log = config.logger
const concurrency = config.languageDetection.concurrency
const { detectLanguage } = require('./language')
const { filterToPredicate } = require('./filter')(r, log)
const { map } = require('./concurrent')

/**
 * Takes a cursor to an object with a 'text' property, detects the language and writes
 * the resulting language code to the object's 'language' property
 *
 * @param metrics {Object}
 * @param cursor {Object}
 * @returns promise {Promise}
 */
async function detectAndUpdate (cursor) {
  const metrics = {}

  const fn = row => detectLanguage(row.text)
    .then(([language]) => {
      const code = language.code
      metrics[code] = metrics[code] ? metrics[code] + 1 : 1
      return r.db('veidemann').table('extracted_text').get(row.warcId).update({ language: code }).run()
    })
    .catch((err) => log.warn('language detection failed for warcId: ', row.warcId, err.message))

  await map(cursor, fn, { concurrency })

  return metrics
}

async function detectLanguages (detectAll) {
  const log = await r.db('maalfrid').table('system').insert({ startTime: r.now(), type: 'languageDetection' }).run()
  const id = log['generated_keys'][0]

  let query = r.db('veidemann').table('extracted_text')
  if (!detectAll) {
    query = query.filter(r.row.hasFields('language').not())
  }
  const cursor = await query.run({ cursor: true })

  // deliberatly not await promise
  detectAndUpdate(cursor)
    .then(metrics => r.db('maalfrid').table('system').update({ id, endTime: r.now(), metrics }).run())
    .catch((err) => r.db('maalfrid').table('system').update({ id, endTime: r.now(), error: err.message }).run())

  return r.db('maalfrid').table('system').get(id).run()
}

function updateEntities (name, labels) {
  let query = r.db('veidemann').table('config_crawl_entities')
  query = labels.reduce((query, label) => query.filter((doc) => doc('meta')('label').contains(label)), query)
  if (name) {
    query = query.filter({ meta: { name } })
  }
  return query.forEach((entity) => r.db('maalfrid').table('entities').insert(entity, { conflict: 'update' })).run()
}

function updateSeeds () {
  return r.db('maalfrid').table('seeds').insert(
    r.db('veidemann').table('config_seeds')
      .getAll(r.args(r.db('maalfrid').table('entities').getField('id').coerceTo('array')), { index: 'entityId' })
    , { conflict: 'update' })
    .run()
}

/**
 * Take all executions having a seedId matching any revelant (public sector) seed,
 * join it with crawl_log and extracted_text data and
 * write it to an aggregation table grouped on executionId and jobExecutionId
 *
 * @returns {Promise}
 */
async function generateAggregate (startTime, endTime) {
  const lowerBound = startTime || await findAggregateLowerBound()
  const upperBound = endTime || await findAggregateUpperBound()

  const result = await r.db('maalfrid').table('system').insert({
    startTime: r.now(),
    type: 'aggregation',
    lowerBound,
    upperBound
  })
  const id = result['generated_keys'][0]

  // deliberatly not await promise
  createAggregate(lowerBound, upperBound)
    .then(result => {
      return r.db('maalfrid').table('system').update({ id, result, endTime: r.now() }).run({ noreply: true })
    })
    .catch((error) => {
      return r.db('maalfrid').table('system').update({ id, error: error.message }).run({ noreply: true })
    })

  return r.db('maalfrid').table('system').get(id).run()
}

/**
 * Find lowerbound date
 *
 * @returns {Promise<Date|*>}
 */
async function findAggregateLowerBound () {
  const aggregations = await r.db('maalfrid').table('system')
    .filter({ type: 'aggregation' })
    .orderBy(r.desc('startTime')).run()
  if (aggregations.length === 0) {
    return new Date(0)
  }
  const lastAggregation = aggregations[0]
  if (!lastAggregation.hasOwnProperty('endTime')) {
    throw new Error('Aggregation already in progress')
  } else {
    return lastAggregation.upperBound
  }
}

/**
 * Find the time of the earliest started jobExecution still running
 *
 * @returns {Promise<Date>}
 */
async function findAggregateUpperBound () {
  const jobExecutionStates = await r.db('veidemann').table('job_executions')
    .orderBy('startTime')
    .pluck('startTime', 'state').run()
  const found = jobExecutionStates.find((elem) => elem.state === 'RUNNING')
  return found !== undefined ? found.startTime : new Date()
}

/**
 *
 * @param lowerBound {Date} lower bound of execution start time
 * @param upperBound {Date} upper bound of execution end time
 * @returns {Promise<void | *>}
 */
async function createAggregate (lowerBound, upperBound) {
  return r.db('maalfrid').table('aggregate').insert(
    // seeds joined with executions
    r.db('maalfrid').table('seeds').pluck('id')
      .eqJoin('id', r.db('veidemann').table('executions'), { index: 'seedId' })
      // discard seeds
      .getField('right')
      .withFields('id', 'startTime', 'endTime', 'state', 'jobExecutionId', 'seedId')
      // only executions in state FINISHED/ABORTED_TIMEOUT/ABORTED_MANUAL
      .filter(r.row('state').eq('FINISHED').or(r.row('state').eq('ABORTED_TIMEOUT')).or(r.row('state').eq('ABORTED_MANUAL')))
      // only executions not already aggregated
      .filter(r.row('startTime').during(lowerBound, upperBound))
      // join with job executions
      .eqJoin('jobExecutionId', r.db('veidemann').table('job_executions'))
      // only job executions in state FINISHED or ABORTED_MANUAL
      .filter(r.row('right')('state').eq('FINISHED').or(r.row('right')('state').eq('ABORTED_MANUAL')))
      // discard job executions
      .getField('left')
      // join with crawl log
      .eqJoin('id', r.db('veidemann').table('crawl_log'), { index: 'executionId' })
      .without({
        // discarded from set of execution fields
        left: ['id', 'state'],
        // discarded from set of crawl log fields
        right: [
          'fetchTimeMs',
          'fetchTimeStamp',
          // 'timeStamp',
          'ipAddress',
          'blockDigest',
          'payloadDigest',
          'statusCode',
          'storageRef',
          'surt'
        ]
      })
      .zip()
      // join with extracted texts
      .eqJoin(r.branch(r.row.hasFields('warcRefersTo'), r.row('warcRefersTo'), r.row('warcId')), r.db('veidemann').table('extracted_text'))
      // discard the text and warcId
      .without({ right: ['text', 'warcId'] })
      .zip()
      // only with language field
      .filter(r.row.hasFields('language'))
  ).run()
}

async function processAggregate (startTime, endTime) {
  if (startTime && endTime) {
    console.log('deleteing')
    await r.db('maalfrid').table('filter').filter(r.row('startTime').during(startTime, endTime)).delete().run()
  }

  const globalPredicate = filterToPredicate(await r.db('maalfrid').table('filter').get('global')('filters').run())

  const timePredicate = startTime && endTime
    ? r.row('startTime').during(startTime, endTime)
    : undefined

  const cursor = await r.db('maalfrid').table('seeds').pluck('id', 'entityId').run({ cursor: true })
  cursor.eachAsync(async ({ id, entityId }) => {
    const seedId = id

    // get the filter for this seed id (if any)
    const seedFilter = await r.db('maalfrid').table('filter').get(seedId).run()
    const seedPredicate = seedFilter && seedFilter.hasOwnProperty('filters')
      ? filterToPredicate(seedFilter['filters'])
      : undefined

    let query = r.db('maalfrid').table('aggregate').getAll(seedId, { index: 'seedId' })
    if (timePredicate !== undefined) {
      query = query.filter(timePredicate)
    }
    if (seedPredicate !== undefined) {
      query = query.filter(seedPredicate)
    }
    query = query.filter(globalPredicate)

    return query
      .group('executionId')
      .pluck('language', 'wordCount')
      .coerceTo('array')
      // reduce to an object where the keys are language codes and values are of the form:
      // [<total number of texts for language>, <number of short texts for language>]
      .fold({}, (acc, curr) => {
        const code = curr('language')
        const wc = curr('wordCount')
        const short = r.branch(wc.lt(3500), 1, 0)
        return r.branch(
          acc.hasFields(code),
          acc.merge(r.object(code, [acc(code)(0).add(1), acc(code)(1).add(short)])),
          acc.merge(r.object(code, [1, short]))
        )
      })
      .ungroup()
      .map(g => {
        const executionId = g('group')
        const statistic = g('reduction')
        const row = r.db('maalfrid').table('aggregate').getAll(executionId, { index: 'executionId' }).nth(0)
        const jobExecutionId = row('jobExecutionId')
        const endTime = row('endTime')
        return { entityId, seedId, executionId, statistic, jobExecutionId, endTime }
      })
      .forEach(statistic => r.db('maalfrid').table('statistics').insert(statistic))
      .run()
  })
}

module.exports = {
  detectLanguages,
  updateSeeds,
  updateEntities,
  generateAggregate,
  processAggregate
}
