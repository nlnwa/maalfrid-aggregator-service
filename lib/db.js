const { r } = require('rethinkdb-ts')

const { logger, languageDetection, rethinkdb: dbConfig } = require('./config')
const log = logger

const { detectLanguage } = require('./language')
const { filtersToPredicate, intervalize } = require('./filter')(r, log)
const { map } = require('./concurrent')
const { InProgressError, NotFoundError } = require('./error')

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
const VEIDEMANN_TABLE_CONFIG = 'config'

/**
 * Enum of operation types
 *
 * @enum {string}
 */
const TYPE = {
  LANGUAGE_DETECTION: 'languageDetection',
  AGGREGATION: 'aggregation',
  STATISTICS: 'statistics',
  SYNC: 'sync'
}

/**
 * Connect to database
 *
 * @returns {Promise<MasterPool>} Connection
 */
async function connect () {
  return r.connectPool(dbConfig)
}

/**
 * Disconnect from database
 *
 * @param {MasterPool} connection
 * @returns {Promise<void>}
 */
async function disconnect (connection) {
  if (connection) {
    return connection.drain({ noreplyWait: true }).catch(() => {})
  }
}

/**
 * Check if type of operation is in progress.
 *
 * If it is in progress throw an error, else mark type as in progress with timestamp.
 *
 *
 * @param {string} type Type of operation
 * @returns {Promise<RDatum<WriteResult<any>>>}
 */
async function setInProgress (type) {
  const result = await r.db(MAALFRID_DB).table(MAALFRID_TABLE_SYSTEM)
    .get('inProgress')
    .update((doc) => r.branch(
      // if in progress
      doc(type),
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
  }
}

/**
 * Set type of operation as not in progress
 *
 * @param {string} type Type of operation
 * @returns {Promise<RDatum<WriteResult<any>>>}
 */
async function setNotInProgress (type) {
  return r.db(MAALFRID_DB).table(MAALFRID_TABLE_SYSTEM).get('inProgress').update(r.object(type, null)).run()
}

/**
 * Insert a log entry for start of operation
 *
 * @param {string} type Type of operation
 * @param {object} [meta] Additional metadata to store in the log entry
 * @returns {Promise<string>} id Id of log entry
 */
async function logStart (type, meta) {
  const logEntry = Object.assign({ startTime: r.now(), type }, meta || {})
  const changes = await r.db(MAALFRID_DB).table(MAALFRID_TABLE_SYSTEM).insert(logEntry).run()
  return changes.generated_keys[0]
}

/**
 * Update a log entry with metadata and endTime
 *
 * @param {string} id Id of log entry
 * @param {object} [meta] Additional metadata to store in the log entry
 * @returns {Promise<RDatum<WriteResult<any>>>}
 */
async function logEnd (id, meta) {
  return r.db(MAALFRID_DB).table(MAALFRID_TABLE_SYSTEM)
    .update(Object.assign({ id, endTime: r.now() }, meta || {}))
    .run()
}

/**
 * Get log from system table
 *
 * @param {string} id Id of log entry
 * @returns {Promise<RSingleSelection<object>>}
 */
async function getLog (id) {
  return r.db(MAALFRID_DB).table(MAALFRID_TABLE_SYSTEM).get(id).run()
}

/**
 * Detect languages of extracted texts
 *
 * @param {boolean} detectAll A boolean indicating whether already detected texts should be processed
 * @param {boolean} wait A boolean indicating if function should await job or not
 * @returns {Promise<string | object>} id of log entry or log entry object
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
 * @param {RTable<any>} query
 * @returns {RSelection<any>}
 */
function filterWithoutLanguages (query) {
  return query.filter(r.row.hasFields('language').not())
}

/**
 * Takes a cursor to an object with a 'text' property, detects the language and writes
 * the resulting language code to the object's 'language' property
 *
 * @param {RCursor<any>} cursor - a cursor to documents containing a 'text' property
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

/**
 *
 * @param {RTable<any>} table
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
 * Given a label synchronize seeds from veidemann to målfrid
 *
 * @param labels A list of labels
 * @returns {Promise<WriteResult<any>>}
 */
async function syncSeeds (labels) {
  const seeds = filterMetaByLabels(r.db(VEIDEMANN_DB).table(VEIDEMANN_TABLE_SEEDS), labels)
  return r.db(MAALFRID_DB).table(MAALFRID_TABLE_SEEDS).insert(seeds, { conflict: 'replace' }).run()
}

/**
 * Check if given job execution id exists
 *
 * @param jobExecutionId
 * @returns {Promise<boolean>}
 */
async function jobExecutionIdExists (jobExecutionId) {
  return !!(await r.db(VEIDEMANN_DB).table(VEIDEMANN_TABLE_JOB_EXECUTIONS).get(jobExecutionId).run())
}

async function getCrawlJobId (label = 'scope:statlige') {
  const key = label.split(':')
  return r.db(VEIDEMANN_DB).table(VEIDEMANN_TABLE_CONFIG)
    .getAll(key, { index: 'label' })
    .filter({ kind: 'crawlJob' })
    .nth(0)
    .getField('id').run()
}

async function getJobExecutionId (crawlJobId) {
  return r.db(VEIDEMANN_DB).table(VEIDEMANN_TABLE_JOB_EXECUTIONS)
    .getAll(crawlJobId, { index: 'jobId' })
    .orderBy('jobId_startTime')
    .nth(0)
    .getField('id').run()
}

/**
 * Combine documents from the veidemann tables extracted_text, executions, job_executions and crawl_log
 *
 * @param {string} jobExecutionId
 * @returns {*}
 */
function aggregateTexts (jobExecutionId) {
  const jobExecutionSeedId = r.db(MAALFRID_DB).table(MAALFRID_TABLE_SEEDS)
    .getField('id')
    .map(seedId => [jobExecutionId, seedId])
    .coerceTo('array')

  const executions = r.db(VEIDEMANN_DB).table(VEIDEMANN_TABLE_EXECUTIONS)
    .getAll(r.args(jobExecutionSeedId), { index: 'jobExecutionId_seedId' })

  return executions
    .withFields('id', 'startTime', 'endTime', 'jobExecutionId', 'seedId')
    // join with crawl log
    .eqJoin('id', r.db(VEIDEMANN_DB).table(VEIDEMANN_TABLE_CRAWL_LOG), { index: 'executionId' })
    .without({
      // discarded from set of execution fields
      left: ['id'],
      // discarded from set of crawl log fields
      right: [
        'fetchTimeMs',
        'fetchTimeStamp',
        'ipAddress',
        'blockDigest',
        'collectionFinalName',
        'payloadDigest',
        'statusCode',
        'storageRef',
        'surt'
      ]
    })
    .zip()
    // join with extracted texts
    .eqJoin(r.branch(r.row.hasFields('warcRefersTo'), r.row('warcRefersTo'), r.row('warcId')),
      r.db(VEIDEMANN_DB).table(VEIDEMANN_TABLE_EXTRACTED_TEXT))
    // discard text and warcId from extracted text
    .without({ right: ['text', 'warcId'] })
    .zip()
}

/**
 * Take all executions having a seedId matching any relevant (public sector) seed,
 * join it with crawl_log and extracted_text data and
 * write it to an aggregation table grouped on executionId and jobExecutionId
 *
 * @param {string} jobExecutionId
 * @param {boolean} wait
 * @returns {Promise<string|Promise<*>>} id of log entry or log entry
 */
async function generateAggregate (jobExecutionId, wait = true) {
  const type = TYPE.AGGREGATION

  if (jobExecutionId === undefined) {
    jobExecutionId = await getJobExecutionId(getCrawlJobId())
  } else if (!(await jobExecutionIdExists(jobExecutionId))) {
    throw new NotFoundError('Could not find job execution with id:', jobExecutionId)
  }

  await setInProgress(type)

  const logId = await logStart(type, { jobExecutionId })

  const promise = r.db(MAALFRID_DB).table(MAALFRID_TABLE_AGGREGATE).insert(aggregateTexts(jobExecutionId)).run()
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
 * Warning: if lowerBound and upperBound are both undefined, then everything will be deleted (unless seedId is
 * specified, which will delete everything with that seedId) from the statistics table
 *
 * @param {string} seedId
 * @param {string} jobExecutionId
 * @returns {Promise<RDatum<WriteResult<any>>>}
 */
async function deleteStatistics (jobExecutionId, seedId) {
  return r.db(MAALFRID_DB).table(MAALFRID_TABLE_STATISTICS)
    .filter(jobExecutionId ? { jobExecutionId } : {})
    .filter(seedId ? { seedId } : {})
    .delete()
    .run()
}

/**
 *
 * @param {string} seedId
 * @returns {RSelection<any>}
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
 * Given a (ReQL) selection of aggregate documents, calculate statistics per language (total count, and short text count)
 *
 * @param {RSelection<any>} selection - reQL selection object
 * @param {string} entityId
 * @param {string} seedId
 * @returns {RDatum<object[]>} reQL selection
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
  return filterSets.filter(sf => Object.prototype.hasOwnProperty.call(sf, 'filters') && sf.filters.length)
}

/**
 *
 * @param {string} jobExecutionId
 * @param {string} seedId
 * @returns {Promise<void>}
 */
async function processAggregate (jobExecutionId, seedId) {
  /**
   * Predicates common for all seeds (global and time)
   *
   * @type {(RDatum<boolean> | object)[]}
   */
  const commonPredicates = [{ jobExecutionId }]

  const globalFilterSet = await getGlobalFilterSet()
  if (globalFilterSet.filters.length) {
    commonPredicates.push(filtersToPredicate(globalFilterSet.filters))
  }

  const cursor = seedId
    ? await r.db(MAALFRID_DB).table(MAALFRID_TABLE_SEEDS).getAll(seedId).getCursor()
    : await r.db(MAALFRID_DB).table(MAALFRID_TABLE_SEEDS).getCursor()

  return cursor.eachAsync(async ({ id: seedId, seed: { entityRef: { id: entityId } } }) => {
    // A selection of aggregate rows (for seed) filtered with common predicates
    const baseSelection = commonPredicates.reduce((query, predicate) => query.filter(predicate), getAggregateBySeedId(seedId))

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
 * @param {string} jobExecutionId
 * @param {string} seedId
 * @param {boolean} wait
 * @returns {Promise<string|Promise<*>>}
 */
async function generateStatistics (jobExecutionId, seedId, wait = true) {
  const type = TYPE.STATISTICS

  if (jobExecutionId === undefined || !(await getJobExecutionId(jobExecutionId))) {
    throw new NotFoundError('Could not find job execution with id:', jobExecutionId)
  }

  await setInProgress(type)

  const logId = await logStart(type, { jobExecutionId })

  const promise = deleteStatistics(jobExecutionId, seedId)
    .then(() => processAggregate(jobExecutionId, seedId))
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

module.exports = {
  connect,
  disconnect,
  detectLanguages,
  syncSeedsAndEntities,
  generateAggregate,
  generateStatistics
}
