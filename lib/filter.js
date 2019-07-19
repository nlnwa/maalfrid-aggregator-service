/**
 * @type {R}
 */
let r

/**
 * @type {{
 *   warn: function(...*): void
 * }}
 */
let log

/**
 *
 * @param {string} str
 * @returns {RDatum}
 */
const prefixRegexp = (str) => r.add('^', str)

/**
 *
 * @param {string} field
 * @returns {Predicate}
 */
const prefixOf = (field) => (str) => field.match(prefixRegexp(str))
const equal = (field) => (str) => field.eq(str)

// filter functions
/**
 *
 * @param {string[]} array
 * @returns {function(=*): RDatum<boolean>}
 */
const someInArrayIsPrefixOf = (array) => (str) => r.expr(array).contains(prefixOf(str))

/**
 *
 * @param {Array<string | number>} array
 * @returns {function(=*): RDatum<boolean>}
 */
const someInArrayEquals = (array) => (field) => r.expr(array).contains(equal(field))

/**
 *
 * @param {number} lowerBound
 * @param {number} upperBound
 * @returns {function(=*): RDatum<boolean>}
 */
const inClosedInterval = ([lowerBound, upperBound]) => (number) => r.and(number.ge(lowerBound), number.le(upperBound))
/**
 *
 * @param {string} regexp
 * @returns {function(=*): RDatum<boolean>}
 */
const matchRegexp = (regexp) => (field) => r.expr(field).match(regexp)
const undefinedFn = (name) => () => () => {
  log.warn('filter named \'' + name + '\' is not implemented')
  return true
}

const filterFnMap = {
  language: someInArrayEquals,
  contentType: someInArrayIsPrefixOf,
  discoveryPath: someInArrayEquals,
  recordType: someInArrayEquals,
  requestedUri: someInArrayIsPrefixOf,
  lix: inClosedInterval,
  characterCount: inClosedInterval,
  longWordCount: inClosedInterval,
  sentenceCount: inClosedInterval,
  wordCount: inClosedInterval,
  matchRegexp
}

const filterFn = (name) => filterFnMap[name] || undefinedFn(name)

/**
 * Map a filter to a reQL predicate function
 *
 * @param {Filter} filter
 * @returns {function(=*): RDatum<boolean>}
 */
const filterToPredicate = (filter) => (doc) => {
  const fn = filterFn(filter.name)(filter.value)(doc.getField(filter.field || filter.name))
  return filter.exclusive ? r.not(fn) : fn
}

/**
 * Map an array of filters to an array of reQL predicate functions
 *
 * @param {Filter[]} filters - array of filters
 * @returns {Predicate[]} array of predicate functions
 */
function filtersToPredicates (filters) {
  return filters.length ? filters.map(filterToPredicate) : [() => true]
}

/**
 * Map an array of filters to a single reQL predicate function
 *
 * @param {Filter[]} filters - array of filters
 * @returns {Predicate} predicate function
 */
function filtersToPredicate (filters) {
  return (doc) => filtersToPredicates(filters)
    .map(p => p(doc))
    .reduce((acc, curr) => acc.and(curr))
}

const {
  compareAsc,
  areRangesOverlapping,
  isEqual
} = require('date-fns')

const minDate = new Date(-1e13)
const maxDate = new Date(1e14)

/**
 * Split filterSets into intervals
 *
 * @param {FilterSet[]} filterSets
 * @returns {FilterInterval[]}
 */
function intervalize (filterSets) {
  if (filterSets.length < 1) {
    return []
  }
  // every date (from/to) of every filterSet is an interval intersection point
  const intersection = filterSets
    .map(filterSet => filterSet.validFrom || minDate)
    .concat(filterSets.map(filterSet => filterSet.validTo || maxDate))
    // remove duplicates, i.e. only insert same date once
    .reduce((acc, curr) => {
      if (acc.length === 0 || !acc.some(date => isEqual(date, curr))) {
        return acc.concat(curr)
      }
      return acc
    }, [])
    .sort(compareAsc)
    .map(date => isEqual(date, minDate) || isEqual(date, maxDate) ? undefined : date)

  // create intervals from the intersection point
  const intervals = intersection.reduce((acc, date, index, array) => {
    if (index > 0 && date) {
      acc[acc.length - 1].to = date
    }
    if (index < array.length - 1) {
      acc.push(Object.assign({ ids: [] }, date ? { from: date } : {}))
    }
    return acc
  }, [])

  // place filterSet id in correct interval
  filterSets
    .sort((a, b) => compareAsc(a.validFrom || minDate, b.validFrom || maxDate))
    .forEach((filterSet) =>
      intervals.forEach(interval => {
        if (areRangesOverlapping(interval.from || minDate, interval.to || maxDate, filterSet.validFrom || minDate, filterSet.validTo || maxDate)) {
          interval.ids.push(filterSet.id)
        }
      })
    )

  return intervals
}

/**
 * @param {R} reql Instance of rethinkdb driver)
 * @param {{warn: Function}} logger Logger instance
 */
module.exports = (reql, logger) => {
  r = reql
  log = logger
  return {
    filterToPredicate,
    filtersToPredicate,
    filtersToPredicates,
    filterFn,
    prefixOf,
    someInArrayEquals,
    someInArrayIsPrefixOf,
    matchRegexp,
    inClosedInterval,
    intervalize
  }
}
