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
 * @returns {function} predicate function
 */
function filterToPredicate (filters) {
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
    inClosedInterval
  }
}
