let r // reQL namespace
let log

const prefixRegexp = (str) => r.add('^', str)
const prefixOf = (field) => (str) => field.match(prefixRegexp(str))
const equal = (field) => (str) => field.eq(str)

const someInArrayIsPrefixOf = (array) => (str) => r.expr(array).contains(prefixOf(str))
const someInArrayEquals = (array) => (field) => r.expr(array).contains(equal(field))
const inClosedInterval = ([lowerBound, upperBound]) => (number) => r.and(number.gte(lowerBound), number.lte(upperBound))

const undefinedFn = (name) => (field) => {
  log.warn('filter named \'' + name + '\' is not implemented')
  return true
}

const filterFnMap = Object.freeze({
  language: someInArrayEquals,
  contentType: someInArrayIsPrefixOf,
  discoveryPath: someInArrayEquals,
  requestedUri: someInArrayIsPrefixOf,
  lix: inClosedInterval,
  characterCount: inClosedInterval,
  longWordCount: inClosedInterval,
  sentenceCount: inClosedInterval,
  wordCount: inClosedInterval
})

const filterFn = (name) => filterFnMap[name] || undefinedFn(name)

const makeFilterPredicate = (filterName, filterValue) => (doc) => filterFn(filterName)(filterValue)(doc.getField(filterName))

/**
 * Transform stored filter values to a reQL object
 *
 * @param selection {selection|stream|array} reQL query object e.g. r.db('dbName').table('tableName')
 * @param filters {object} filters to apply to given reQL selection
 * @returns {selection|stream|array} reQL query object
 */
function filterToReql (selection, filters) {
  return Object.keys(filters).reduce((selection, name) => selection.filter(makeFilterPredicate(name, filters[name])), selection)
}

/**
 * @param reql {object} reQL namespace (instance of rethinkdb driver)
 * @param logger {{warn: Function}} logger instance
 */
module.exports = (reql, logger) => {
  r = reql
  log = (logger && logger.hasOwnProperty('warn')) || {warn: () => {}}

  return {
    filterToReql,
    makeFilterPredicate,
    filterFn,
    prefixOf,
    someInArrayEquals,
    someInArrayIsPrefixOf,
    inClosedInterval
  }
}
