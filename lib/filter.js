let r // reQL namespace
let log

const prefixRegexp = (str) => r.add('^', str)
const prefixOf = (field) => (str) => field.match(prefixRegexp(str))
const equal = (field) => (str) => field.eq(str)

const someInArrayIsPrefixOf = (array) => (str) => r.expr(array).contains(prefixOf(str))
const someInArrayEquals = (array) => (field) => r.expr(array).contains(equal(field))
const inClosedInterval = ([lowerBound, upperBound]) => (number) => r.and(number.gte(lowerBound), number.lte(upperBound))
const matchRegexp = (regexp) => (field) => !!r.expr(field).match(regexp)

const undefinedFn = (name) => () => () => {
  log.warn('filter named \'' + name + '\' is not implemented')
  return true
}

const filterFnMap = {
  language: someInArrayEquals,
  contentType: someInArrayIsPrefixOf,
  discoveryPath: someInArrayEquals,
  requestedUri: someInArrayIsPrefixOf,
  lix: inClosedInterval,
  characterCount: inClosedInterval,
  longWordCount: inClosedInterval,
  sentenceCount: inClosedInterval,
  wordCount: inClosedInterval,
  matchRegexp
}

const filterFn = (name) => filterFnMap[name] || undefinedFn(name)

const makePredicate = (filter) => (doc) => filterFn(filter.name)(filter.value)(doc.getField(filter.field || filter.name))

/**
 * Combine stored filters to a reQL predicate function
 *
 * @param filters {array} filters to apply to given reQL selection
 * @returns {function | boolean} predicate function
 */
function filterToPredicate (filters) {
  if (filters.length === 0) {
    return true
  }
  const predicates = filters.map(makePredicate)
  if (predicates.length === 1) {
    return filters[0].exlusive
      ? (doc) => predicates[0](doc).not()
      : (doc) => predicates[0](doc)
  }

  return (doc) => predicates.reduce((acc, curr, index) => {
    const next = filters[index].exlusive
      ? curr(doc).not()
      : curr(doc)
    if (index === 1) {
      return filters[0].exlusive
        ? acc(doc).not().and(next)
        : acc(doc).and(next)
    } else {
      return acc.and(next)
    }
  })
}

const {
  compareAsc,
  areRangesOverlapping,
  isEqual
} = require('date-fns')

const minDate = -1e14
const maxDate = 1e14

/**
 *
 * @param {Array<{validFrom: string, validTo: string, id: string}>} filterSets
 * @returns {{from: Date, to: Date, ids: string[]}[]}
 */
function intervalize (filterSets) {
  if (filterSets.length < 1) {
    return []
  }
  // every date (from/to) of every filterSet is an interval intersection point
  const intersection = filterSets
    .map(filterSet => new Date(filterSet.validFrom || minDate))
    .concat(filterSets.map(filterSet => new Date(filterSet.validTo || maxDate)))
    // remove duplicates, i.e. only insert same date once
    .reduce((acc, curr) => {
      if (acc.length === 0 || !acc.some(date => isEqual(date, curr))) {
        return acc.concat(curr)
      }
      return acc
    }, [])
    .sort(compareAsc)

  // create intervals from the intersection point
  const intervals = intersection.reduce((acc, date, index, array) => {
    if (index > 0) {
      acc[acc.length - 1].to = date
    }
    if (index < array.length - 1) {
      acc.push({ from: date, to: undefined, ids: [] })
    }
    return acc
  }, [])

  // place filterSet id in correct interval
  filterSets
    .sort((a, b) => compareAsc(new Date(a.validFrom || minDate), new Date(b.validFrom || maxDate)))
    .forEach((filterSet) => {
      const validFrom = new Date(filterSet.validFrom || minDate)
      const validTo = new Date(filterSet.validTo || maxDate)
      intervals.forEach(interval => {
        if (areRangesOverlapping(interval.from, interval.to, validFrom, validTo)) {
          interval.ids.push(filterSet.id)
        }
      })
    })

  return intervals
}

/**
 * @param reql {object} reQL namespace (instance of rethinkdb driver)
 * @param logger {{warn: Function}} logger instance
 */
module.exports = (reql, logger) => {
  r = reql
  log = logger
  return {
    filterToPredicate,
    makePredicate,
    filterFn,
    prefixOf,
    someInArrayEquals,
    someInArrayIsPrefixOf,
    matchRegexp,
    inClosedInterval,
    intervalize
  }
}
