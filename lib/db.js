const { r } = require('rethinkdb-ts')

const config = require('./config')
const log = config.logger
const languageDetection = config.languageDetection

const { detectLanguage } = require('./language')
const { filtersToPredicate, intervalize } = require('./filter')(r, log)
const { map } = require('./concurrent')
const { InProgressError, FailedPreconditionError } = require('./error')

const MAALFRID_DB = 'maalfrid'
const MAALFRID_TABLE_SYSTEM = 'system'
const MAALFRID_TABLE_ENTITIES = 'entities'
const MAALFRID_TABLE_SEEDS = 'seeds'
const MAALFRID_TABLE_AGGREGATE = 'aggregate'
const MAALFRID_TABLE_STATISTICS = 'statistics'
const MAALFRID_TABLE_FILTER = 'filter'

const VEIDEMANN_DB = 'veidemann'
const VEIDEMANN_TABLE_EXTRACTED_TEXT = 'extracted_text'
const VEIDEMANN_TABLE_CRAWL_ENTITIES = 'config_crawl_entities'
const VEIDEMANN_TABLE_SEEDS = 'config_seeds'
const VEIDEMANN_TABLE_JOB_EXECUTIONS = 'job_executions'
const VEIDEMANN_TABLE_EXECUTIONS = 'executions'
const VEIDEMANN_TABLE_CRAWL_LOG = 'crawl_log'

/**
 * @type {MasterPool}
 */
let connectionPool

/**
 * Enum for log entry 'type' field in MAALFRID_TABLE_SYSTEM
 *
 * @enum {string}
 */
const TYPE = {
  LANGUAGE_DETECTION: 'languageDetection',
  AGGREGATION: 'aggregation',
  STATISTICS: 'statistics',
  SYNC: 'sync'
}

const OPERATION_TIMEOUT = 43200 // seconds (12 hours)

async function connect () {
  connectionPool = await r.connectPool(config.rethinkdb)
}

function disconnect () {
  if (connectionPool) {
    connectionPool.drain({ noreplyWait: true }).catch(() => {})
  }
}

/**
 * Detect languages of extracted texts
 *
 * @param {boolean} detectAll A boolean indicating whether already detected texts should be processed
 * @param {boolean} wait A boolean indicating if function should await job or not
 * @returns {Promise<string>} id of log entry
 */
async function detectLanguages (detectAll, wait = true) {
  const type = TYPE.LANGUAGE_DETECTION

  await setInProgress(type)
  const logId = await logStart(type)

  const query = r.db(VEIDEMANN_DB).table(VEIDEMANN_TABLE_EXTRACTED_TEXT)
  const cursor = detectAll
    ? await query.getCursor()
    : await filterWithoutLanguages(query).getCursor()

  const promise = detectAndUpdate(cursor)
    .then((metrics) => logEnd(logId, { metrics }))
    .catch((err) => logEnd(logId, { error: err.message }))
    .finally(() => setNotInProgress(type))

  if (wait) {
    await promise
    return getLog(logId)
  } else {
    return logId
  }
}

/**
 *
 * @param {Selection} query
 * @returns {Selection}
 */
function filterWithoutLanguages (query) {
  return query.filter(r.row.hasFields('language').not())
}

/**
 * Takes a cursor to an object with a 'text' property, detects the language and writes
 * the resulting language code to the object's 'language' property
 *
 * @param {RCursor} cursor - a cursor to documents containing a 'text' property
 * @returns {Promise<Metrics>} a metrics object
 */
async function detectAndUpdate (cursor) {
  const metrics = {}

  /**
   *
   * @param {ExtractedText} row
   * @returns {Promise<void>}
   */
  async function fn (row) {
    try {
      const [detectedLanguage] = await detectLanguage(row.text)
      const language = detectedLanguage.code
      metrics[language] = (metrics[language] || 0) + 1
      return r.db(VEIDEMANN_DB).table(VEIDEMANN_TABLE_EXTRACTED_TEXT).get(row.warcId).update({ language }).run()
    } catch (err) {
      log.warn('language detection failed for warcId: ', row.warcId, err.message)
    }
  }

  // collect a given number of rows from the cursor and concurrently
  // call the given mapper function on the collected rows until cursor is exhausted
  await map(cursor, fn, languageDetection.concurrency)

  return metrics
}

/**
 *
 * @returns {Promise<string>} id of log entry
 */

/**
 * Synchronize the seeds and entities from veidemann to maalfrid
 *
 * @param labels Labels to filter on
 * @param {boolean} wait Indicates if operation is async or not
 * @returns {Promise<string>}
 */
async function syncSeedsAndEntities (labels, wait = true) {
  const type = TYPE.SYNC

  await setInProgress(type)
  const logId = await logStart(type)

  const promise = syncSeeds(labels)
    .then(() => syncEntities())
    .then(result => logEnd(logId, { result }))
    .catch(error => logEnd(logId, { error: error.message }))
    .finally(() => setNotInProgress(type))

  if (wait) {
    await promise
    return getLog(logId)
  } else {
    return logId
  }
}

async function syncEntities () {
  const entityIds = r.db(MAALFRID_DB).table(MAALFRID_TABLE_SEEDS)('seed')('entityRef')('id')
  const entities = r.db(VEIDEMANN_DB).table(VEIDEMANN_TABLE_CRAWL_ENTITIES).getAll(r.args(entityIds.coerceTo('array')))

  return r.db(MAALFRID_DB).table(MAALFRID_TABLE_ENTITIES).insert(entities, { conflict: 'replace' }).run()
}

async function syncSeeds (labels) {
  const seeds = filterMetaByLabels(r.db(VEIDEMANN_DB).table(VEIDEMANN_TABLE_SEEDS), labels)
  return r.db(MAALFRID_DB).table(MAALFRID_TABLE_SEEDS).insert(seeds, { conflict: 'replace' }).run()
}

/**
 *
 * @param {RTable} table
 * @param {Array} labels
 * @returns {RSelection<any>}
 */
function filterMetaByLabels (table, labels) {
  if (!labels || labels.length < 1) {
    return table
  }
  const keyValueForm = labels.map(s => s.toLowerCase()).map(label => label.split(':', 2))
  return table.getAll(...keyValueForm, { index: 'label' })
}

/**
 * Take all executions having a seedId matching any relevant (public sector) seed,
 * join it with crawl_log and extracted_text data and
 * write it to an aggregation table grouped on executionId and jobExecutionId
 *
 * @param {Date} startTime
 * @param {Date} endTime
 * @param {boolean} wait
 * @returns {Promise<string|Promise<*>>} id of log entry or log entry
 */
async function generateAggregate (startTime, endTime, wait = true) {
  const type = TYPE.AGGREGATION

  await setInProgress(type)

  const lowerBound = startTime || await findAggregateLowerBound()
  const upperBound = endTime || await findAggregateUpperBound(lowerBound)

  checkBounds(lowerBound, upperBound)

  const logId = await logStart(type, Object.assign(
    {},
    lowerBound ? { lowerBound } : {},
    upperBound ? { upperBound } : {}))

  const promise = r.db(MAALFRID_DB).table(MAALFRID_TABLE_AGGREGATE).insert(aggregateTexts(lowerBound, upperBound)).run()
    .then(result => logEnd(logId, { result }))
    .catch((error) => logEnd(logId, { error: error.message }))
    .finally(() => setNotInProgress(type))

  if (wait) {
    await promise
    return getLog(logId)
  } else {
    return logId
  }
}

/**
 * Find lowerBound date
 *
 * @returns {Promise<Date>}
 */
async function findAggregateLowerBound () {
  const lastRun = await r.db(MAALFRID_DB).table(MAALFRID_TABLE_SYSTEM)
    .filter({ type: TYPE.AGGREGATION })
    .filter(r.row.hasFields('error').not())
    .orderBy(r.desc('startTime')).limit(1).run()

  if (lastRun.length === 0) {
    return null
  } else {
    return lastRun[0].upperBound
  }
}

/**
 * Find upperBound date
 *
 * @param {Date} lowerBound
 *
 * @returns {Promise<Date>} the start time of the earliest started jobExecution still running
 */
async function findAggregateUpperBound (lowerBound) {
  const jobExecutionStates = await r.db(VEIDEMANN_DB).table(VEIDEMANN_TABLE_JOB_EXECUTIONS)
    .filter(r.row('startTime').gt(lowerBound))
    .orderBy('startTime')
    .pluck('startTime', 'state').run()
  const found = jobExecutionStates.find((elem) => elem.state === 'RUNNING')
  return found !== undefined ? found.startTime : new Date()
}

/**
 * Combine documents from the veidemann tables extracted_text, executions, job_executions and crawl_log
 *
 * @param {Date} lowerBound Lower bound of execution start time
 * @param {Date} upperBound Upper bound of execution end time
 * @returns {Selection | *}
 */
function aggregateTexts (lowerBound, upperBound) {
  return r.db(MAALFRID_DB).table(MAALFRID_TABLE_SEEDS).pluck('id')
    .eqJoin('id', r.db(VEIDEMANN_DB).table(VEIDEMANN_TABLE_EXECUTIONS), { index: 'seedId' })
    // discard seeds
    .getField('right')
    .withFields('id', 'startTime', 'endTime', 'state', 'jobExecutionId', 'seedId')
    // only executions in non-active states
    .filter(doc => r.expr(['CREATED', 'FETCHING', 'SLEEPING']).contains(doc('state')).not())
    // only executions not already aggregated
    .filter(getTimePredicate(lowerBound, upperBound) || true)
    // join with job executions
    .eqJoin('jobExecutionId', r.db(VEIDEMANN_DB).table(VEIDEMANN_TABLE_JOB_EXECUTIONS))
    // only job executions in non-active states
    .filter(doc => r.expr(['CREATED', 'RUNNING']).contains(doc('right')('state')).not())
    // discard job executions
    .getField('left')
    // join with crawl log
    .eqJoin('id', r.db(VEIDEMANN_DB).table(VEIDEMANN_TABLE_CRAWL_LOG), { index: 'executionId' })
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
    .eqJoin(r.branch(r.row.hasFields('warcRefersTo'), r.row('warcRefersTo'), r.row('warcId')), r.db(VEIDEMANN_DB).table(VEIDEMANN_TABLE_EXTRACTED_TEXT))
    // discard the text and warcId
    .without({ right: ['text', 'warcId'] })
    .zip()
}

/**
 *
 * @param {Date} startTime
 * @param {Date} endTime
 * @param {string} seedId
 * @param {boolean} wait
 * @returns {Promise<string|Promise<*>>}
 */
async function generateStatistics (startTime, endTime, seedId, wait = true) {
  const type = TYPE.STATISTICS

  await setInProgress(type)

  const lowerBound = startTime || await findFilterLowerBound()
  const upperBound = endTime || await findFilterUpperBound()

  checkBounds(lowerBound, upperBound)

  const logId = await logStart(type, Object.assign(
    {},
    lowerBound ? { lowerBound } : {},
    upperBound ? { upperBound } : {},
    seedId ? { seedId } : {}))

  const promise = deleteStatistics(lowerBound, upperBound, seedId)
    .then(() => processAggregate(lowerBound, upperBound, seedId))
    .then(() => logEnd(logId))
    .catch(error => logEnd(logId, { error: error.message }))
    .finally(() => setNotInProgress(type))

  if (wait) {
    await promise
    return getLog(logId)
  } else {
    return logId
  }
}

/**
 *
 * @param {Date} lowerBound
 * @param {Date} upperBound
 */
function checkBounds (lowerBound, upperBound) {
  if (lowerBound >= upperBound && !(lowerBound === null && upperBound === null)) {
    throw FailedPreconditionError(`lowerbound (${lowerBound}) is greater than or equal to upperbound (${upperBound}`)
  }
}

/**
 *
 * @returns {Promise<Date>}
 */
async function findFilterLowerBound () {
  /**
   * @type {LogEntry[]}
   */
  const lastRun = await r.db(MAALFRID_DB).table(MAALFRID_TABLE_SYSTEM)
    .filter({ type: TYPE.STATISTICS })
    .filter(r.row.hasFields('seedId').not())
    .filter(r.row.hasFields('error').not())
    .orderBy(r.desc('endTime'))
    .limit(1).run()

  if (lastRun.length > 0) {
    return lastRun[0].upperBound
  } else {
    return null
  }
}

/**
 *
 * @returns {Promise<Date>} the previous aggregation's upperBound
 */
async function findFilterUpperBound () {
  /**
   * @type {LogEntry[]}
   */
  const aggregations = await r.db(MAALFRID_DB).table(MAALFRID_TABLE_SYSTEM)
    .filter({ type: TYPE.AGGREGATION })
    .filter(r.row.hasFields('endTime'))
    .orderBy(r.desc('endTime'))
    .run()
  if (aggregations.length) {
    return aggregations[0].upperBound
  } else {
    return null
  }
}

/**
 * Warning: if lowerBound and upperBound are both undefined, then everything will be deleted (unless seedId is
 * specified, which will delete everything with that seedId) from the statistics table
 *
 * @param {string} seedId
 * @param {Date} lowerBound
 * @param {Date} upperBound
 * @returns {Promise<void>}
 */
async function deleteStatistics (lowerBound, upperBound, seedId) {
  return r.db(MAALFRID_DB).table(MAALFRID_TABLE_STATISTICS)
    .between(lowerBound || r.minval, upperBound || r.maxval, { index: 'endTime' })
    .filter(seedId ? { seedId } : {}).delete().run()
}

/**
 *
 * @param {Date} lowerBound
 * @param {Date} upperBound
 * @param {string} seedId
 * @returns {Promise<void>}
 */
async function processAggregate (lowerBound, upperBound, seedId) {
  /**
   * Predicates common for all seeds (global and time)
   *
   * @type {RDatum<boolean>[]}
   */
  const commonPredicates = []

  const timePredicate = getTimePredicate(lowerBound, upperBound)
  if (timePredicate) {
    commonPredicates.push(timePredicate)
  }
  const globalFilterSet = await getGlobalFilterSet()
  if (globalFilterSet.filters.length) {
    commonPredicates.push(filtersToPredicate(globalFilterSet.filters))
  }

  const cursor = seedId
    ? await r.db(MAALFRID_DB).table(MAALFRID_TABLE_SEEDS).getAll(seedId).getCursor()
    : await r.db(MAALFRID_DB).table(MAALFRID_TABLE_SEEDS).getCursor()

  return cursor.eachAsync(async ({ id: seedId, seed: { entityRef: { id: entityId } } }) => {
    /**
     * A selection of aggregate rows (for seed) filtered with common predicates
     * @type {Selection}
     */
    let baseSelection = commonPredicates.reduce((query, predicate) => query.filter(predicate), getAggregateBySeedId(seedId))

    const seedFilterSets = await getSeedFilterSet(seedId)

    const intervals = intervalize(seedFilterSets)

    if (intervals.length === 0) {
      // add one interval without any filterSets
      intervals.push({ ids: [] })
    }

    for (let i = 0; i < intervals.length; i++) {
      const interval = intervals[i]
      const intervalPredicate = getTimePredicate(interval.from, interval.to)
      const intervalSelection = intervalPredicate === null ? baseSelection : baseSelection.filter(intervalPredicate)

      const seedPredicates = interval.ids.map(id => filtersToPredicate(seedFilterSets.find(sf => sf.id === id).filters))

      const selection = seedPredicates.reduce((selection, predicate) => selection.filter(predicate), intervalSelection)

      await calculateStatistics(selection, entityId, seedId)
        .forEach(statistic => r.db(MAALFRID_DB).table(MAALFRID_TABLE_STATISTICS).insert(statistic)).run()
    }
  })
}

/**
 *
 * @param {string} seedId
 * @returns {Selection}
 */
function getAggregateBySeedId (seedId) {
  return r.db(MAALFRID_DB).table(MAALFRID_TABLE_AGGREGATE).getAll(seedId, { index: 'seedId' })
}

/**
 *
 * @param {Date} startTime
 * @param {Date} endTime
 * @returns {RDatum<boolean>}
 */
function getTimePredicate (startTime, endTime) {
  return startTime && endTime
    ? r.row('startTime').during(startTime, endTime)
    : startTime
      ? r.row('startTime').gt(startTime)
      : endTime
        ? r.row('startTime').lt(endTime)
        : null
}

/**
 *
 * @returns {Promise<FilterSet>}
 */
async function getGlobalFilterSet () {
  return r.db(MAALFRID_DB).table(MAALFRID_TABLE_FILTER).get('global').run()
}

/**
 *
 * @param {string} seedId
 * @returns {Promise<FilterSet[]>}
 */
async function getSeedFilterSet (seedId) {
  const filterSets = await r.db(MAALFRID_DB).table(MAALFRID_TABLE_FILTER).getAll(seedId, { index: 'seedId' }).run()
  return filterSets.filter(sf => sf.hasOwnProperty('filters') && sf.filters.length)
}

/**
 * Given a (ReQL) selection of aggregate documents, calculate statistics per language (total count, and short text count)
 *
 * @param {Selection} selection - reQL selection object
 * @param {string} entityId
 * @param {string} seedId
 * @returns {Selection} - reQL selection
 */
function calculateStatistics (selection, entityId, seedId) {
  return selection
    .group('executionId')
    .map((doc) => r.object(doc.getField('language'), {
      short: r.branch(doc.getField('wordCount').lt(3500), 1, 0),
      total: 1
    }))
    .reduce((left, right) =>
      left.keys().setIntersection(right.keys())
        .do((intersection) =>
          r.branch(
            intersection.count().eq(0),
            left.merge(right),
            left.merge(right).merge(
              intersection.map(language => [language, {
                short: left.getField(language).getField('short').add(right.getField(language).getField('short')),
                total: left.getField(language).getField('total').add(right.getField(language).getField('total'))
              }]).coerceTo('object')))))
    .ungroup()
    .map(g => {
      const executionId = g('group')
      const statistic = g('reduction')
      const row = r.db(MAALFRID_DB).table('aggregate').getAll(executionId, { index: 'executionId' }).nth(0)
      const jobExecutionId = row('jobExecutionId')
      const endTime = row('endTime')
      return { entityId, seedId, executionId, statistic, jobExecutionId, endTime }
    })
}

/**
 * Check if type of operation is in progress.
 *
 * If it is in progress throw an error, else mark type as in progress with timestamp.
 *
 *
 * @param {string} type - type of operation
 * @returns {Promise<void>}
 */
async function setInProgress (type) {
  const result = await r.db(MAALFRID_DB).table(MAALFRID_TABLE_SYSTEM)
    .get('inProgress')
    .update((doc) => r.branch(
      // if is in progress and operation has not timed out
      r.and(doc(type), r.now().sub(doc(type)).lt(OPERATION_TIMEOUT)),
      // then don't update anything
      {},
      // else set in progress to current date
      r.object(type, r.now())), { returnChanges: true })
    .run()

  if (result.errors > 0) {
    throw Error(result.first_error)
  }
  if (result.unchanged) {
    throw InProgressError(type + ' already in progress')
  } else {
    const prev = result.changes[0].old_val[type] || new Date()
    const duration = Date.now() - prev.getTime()

    // has current operation timed out?
    if (duration / 1000 > OPERATION_TIMEOUT) {
      await timeoutOperation(type)
    }
  }
}

/**
 *
 * @param {string} type
 * @returns {Promise<void>}
 */
async function timeoutOperation (type) {
  // previous operation timed out so update log entries of given type who has no endTime field
  await r.db(MAALFRID_DB).table(MAALFRID_TABLE_SYSTEM)
    .filter({ type })
    .filter(r.row.hasFields('endTime').not())
    .update({ endTime: r.now(), error: 'operation timed out or program crashed during previous operation' }).run()
}

/**
 * Set type of operation as not in progress
 *
 * @param {String} type - type of operation
 * @returns {Promise<void>}
 */
async function setNotInProgress (type) {
  return r.db(MAALFRID_DB).table(MAALFRID_TABLE_SYSTEM).get('inProgress').update(r.object(type, null)).run()
}

/**
 * Insert a log entry for start of operation
 *
 * @param {string} type - type of operation
 * @param {Object} [meta] - additional data to store in log entry
 * @returns {Promise<string>} id - id of log entry
 */
async function logStart (type, meta) {
  const logEntry = Object.assign({ startTime: r.now(), type }, meta || {})
  const changes = await r.db(MAALFRID_DB).table(MAALFRID_TABLE_SYSTEM).insert(logEntry).run()
  return changes.generated_keys[0]
}

async function getLog (id) {
  return r.db(MAALFRID_DB).table(MAALFRID_TABLE_SYSTEM).get(id).run()
}

/**
 * Update a log entry with metadata and endTime
 *
 * @param {string} id Id of log entry
 * @param {Object} [meta] Additional metadata to store in the log entry
 * @returns {Promise<void>}
 */
async function logEnd (id, meta) {
  return r.db(MAALFRID_DB).table(MAALFRID_TABLE_SYSTEM)
    .update(Object.assign({ id, endTime: r.now() }, meta || {}))
    .run()
}

module.exports = {
  connect,
  disconnect,
  detectLanguages,
  syncSeedsAndEntities,
  generateAggregate,
  generateStatistics
}
