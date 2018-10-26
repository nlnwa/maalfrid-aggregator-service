const dbConfig = require('./config').rethinkdb
const r = require('rethinkdb-js')(dbConfig)
const config = require('./config')
const log = config.logger
const {detectLanguage} = require('./language')

/**
 * Takes a cursor to an object with a 'text' property, detects the language and writes
 * the resulting language code to the object's 'language' property
 *
 * @param metrics {Object}
 * @param cursor {Object}
 * @returns promise {Promise}
 */
async function detectAndUpdate (cursor, metrics) {
  return cursor.eachAsync((row) => {
    return detectLanguage(row.text)
      .then((results) => results[0])
      .then((result) => {
        const language = result.code

        if (metrics.hasOwnProperty(language)) {
          metrics[language] += 1
        } else {
          metrics[language] = 1
        }

        return r.db('veidemann').table('extracted_text').get(row.warcId).update({language}).run()
      })
      .catch((err) => log.warn('language detection failed for warcId: ', row.warcId, err.message))
  })
}

async function detectAndUpdateTextsMissingCode () {
  const log = await r.db('maalfrid').table('system').insert({startTime: r.now(), type: 'languageDetection'}).run()
  const id = log['generated_keys'][0]

  const cursor = await r.db('veidemann').table('extracted_text')
    .filter(r.row.hasFields('language').not()).run({cursor: true})

  const metrics = {}

  // deliberatly not await promise
  detectAndUpdate(cursor, metrics)
    .then(() => Object.keys(metrics).reduce((count, curr) => count + metrics[curr], 0))
    .then((count) => r.db('maalfrid').table('system').update({id, endTime: r.now(), metrics, count}).run())
    .catch((error) => r.db('maalfrid').table('system').update({id, endTime: r.now(), metrics, error}).run())

  return r.db('maalfrid').table('system').get(id).run()
}

function updateEntities () {
  return r.db('veidemann').table('config_crawl_entities')
    .filter((doc) => doc('meta')('label').contains({key: 'Group', value: 'Språkrådet'}))
    .forEach((entity) => r.db('maalfrid').table('entities').insert(entity, {conflict: 'replace'}))
    .run()
}

function updateSeeds () {
  return r.db('maalfrid').table('seeds').insert(
    r.db('veidemann').table('config_seeds')
      .getAll(r.args(r.db('maalfrid').table('entities').getField('id').coerceTo('array')), {index: 'entityId'})
    , {conflict: 'replace'})
    .run()
}

/**
 * Take all executions having a seedId matching any revelant (public sector) seed,
 * join it with crawl_log and extracted_text data and
 * write it to an aggregation table grouped on executionId and jobExecutionId
 *
 * @returns {Promise}
 */
async function generateAggregate () {
  const lowerBound = await findAggregateLowerBound()
  const upperBound = await findAggregateUpperBound()

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
      return r.db('maalfrid').table('system').update({id, result, endTime: r.now()}).run({noreply: true})
    })
    .catch((error) => {
      return r.db('maalfrid').table('system').update({id, error}).run({noreply: true})
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
    .filter({type: 'aggregation'})
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
 * Find upperbound date
 *
 * @returns {Promise<Date>}
 */
// find the time of the earliest started jobExecution still running
async function findAggregateUpperBound () {
  const jobExecutionStates = await r.db('veidemann').table('job_executions')
    .orderBy('startTime')
    .pluck('startTime', 'state').run()
  const found = jobExecutionStates.find((elem) => elem.state === 'RUNNING')
  return found !== undefined ? found.startTime : new Date()
}

/**
 *
 * @param lowerBound {Date}
 * @param upperBound {Date}
 * @returns {Promise<void | *>}
 */
async function createAggregate (lowerBound, upperBound) {
  return r.db('maalfrid').table('aggregate').insert(
    // seeds joined with executions
    r.db('maalfrid').table('seeds').pluck('id')
      .eqJoin('id', r.db('veidemann').table('executions'), {index: 'seedId'})
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
      .eqJoin('id', r.db('veidemann').table('crawl_log'), {index: 'executionId'})
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
      .without({right: ['text', 'warcId']})
      .zip()
      // only with language field
      .filter(r.row.hasFields('language'))
  ).run()
}

module.exports = {
  detectAndUpdateTextsMissingCode,
  updateSeeds,
  updateEntities,
  generateAggregate
}
