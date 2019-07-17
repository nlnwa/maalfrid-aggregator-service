const { describe, it, before } = require('mocha')
const { assert } = require('chai')

const reqlMock = {
  expr: (value) => value,
  add: (a, b) => new RegExp(a.concat(b)),
  and: (a, b) => a && b,
  not: (bool) => !bool
}

const {
  someInArrayEquals,
  someInArrayIsPrefixOf,
  inClosedInterval,
  matchRegexp,
  prefixOf,
  filterFn,
  filterToPredicate,
  filtersToPredicates,
  filtersToPredicate,
  intervalize
} = require('../lib/filter')(reqlMock, { warn: () => {} })

/**
 *
 * @type {FilterSet}}
 */
const filterSet = require('./filter').filterSets[0]
const filterSets = require('./filter').filterSets.map(fs => {
  if (Object.prototype.hasOwnProperty.call(fs, 'validTo')) {
    fs.validTo = new Date(fs.validTo)
  }
  if (Object.prototype.hasOwnProperty.call(fs, 'validFrom')) {
    fs.validFrom = new Date(fs.validFrom)
  }
  return fs
})
/**
 * @type {Aggregate[]}
 */
const data = require('./filter.data')

describe('lib/filter.js', () => {
  before('monkeypatch built-ins to emulate reql object functions', () => {
    /* eslint-disable no-extend-native */

    Array.prototype.contains = Array.prototype.some

    String.prototype.eq = function (str) { return this.valueOf() === str }

    // (reQL) match returns null or array, but for ease of we return true or false
    const match = String.prototype.match
    String.prototype.match = function (regexp) {
      return !!match.call(this, regexp)
    }

    Object.prototype.getField = function getField (field) {
      return this[field] || ''
    }

    Number.prototype.gt = function (number) { return this.valueOf() > number }
    Number.prototype.ge = function (number) { return this.valueOf() >= number }
    Number.prototype.lt = function (number) { return this.valueOf() < number }
    Number.prototype.le = function (number) { return this.valueOf() <= number }

    Boolean.prototype.and = function (val) { return this.valueOf() && val }
    Boolean.prototype.not = function () { return !this.valueOf() }
  })

  describe('spec', () => {
    describe('r', () => {
      it('r.add should return regexp of concatenated strings', () => {
        const expected = '/ab'.concat('cd/')
        const actual = reqlMock.add('ab', 'cd').toString()

        assert.equal(expected, actual)
      })

      it('r.expr is the identity function', () => {
        assert.equal(reqlMock.expr(true), true)
        assert.deepEqual(reqlMock.expr({ 1: 2 }), { 1: 2 })
      })

      it('r.and is the and of two booleans', () => {
        assert.isTrue(reqlMock.and(true, true))
        assert.isFalse(reqlMock.and(true, false))
      })

      it('r.not negates the argument', () => {
        assert.isTrue(reqlMock.not(false))
        assert.isFalse(reqlMock.not(true))
      })
    })

    describe('Array.prototype', () => {
      it('contains should alias some', () => {
        assert.isTrue([1, 2, 3].contains(e => e === 2))
        assert.isFalse(['a', 'b', 'c'].contains(f => f === 'd'))
      })
    })

    describe('String.prototype', () => {
      it('match returns false or true', () => {
        assert.equal('abcd'.match(/abcd/), true)
        assert.equal('abcd'.match(/abd/), false)
      })
    })

    describe('Object.prototype', () => {
      it('getField should return named property of object', () => {
        const expected = 2
        const obj = { expected }
        const actual = obj.getField('expected')
        assert.equal(expected, actual)
      })
    })

    describe('Number.prototype', () => {
      it('gt', () => {
        assert.isTrue(Number(1).gt(0))
        assert.isFalse(Number(1).gt(1))
        assert.isFalse(Number(1).gt(2))
      })
      it('lt', () => {
        assert.isTrue(Number(0).lt(1))
      })
      it('ge', () => {
        assert.isTrue(Number(3).ge(2))
        assert.isTrue(Number(3).ge(3))
      })
      it('le', () => {
        assert.isTrue(Number(1).le(2))
        assert.isTrue(Number(1).le(1))
      })
    })

    describe('Boolean.prototype', () => {
      it('and', () => {
        assert.isFalse(false.and(true))
        assert.isFalse(true.and(false))
        assert.isTrue(true.and(true))
      })

      it('not', () => {
        assert.isTrue(false.not())
        assert.isFalse(true.not())
      })
    })
  })

  describe('prefixOf', () => {
    it('should return a function when given an argument', () => {
      assert.isFunction(prefixOf('hello'))
    })

    it('returned function should return falsy when called with an argument which is not a prefix', () => {
      assert.isFalse(prefixOf('hello')('go'))
    })

    it('returned function should return truthy when called with an argument that is a prefix', () => {
      const result = prefixOf('golang')('go')
      assert.isTrue(result)
    })
  })

  describe('someInArrayEquals', () => {
    it('should return a function when given an argument', () => {
      assert.isFunction(someInArrayEquals(['NNO', 'NOB']))
    })

    it('returned function should return boolean when given an array argument', () => {
      assert.isBoolean(someInArrayEquals([1, 2, 3])('NNO'))
    })

    it('returned function should throw when given an undefined argument', () => {
      assert.throws(someInArrayEquals([1, 2, 3]), TypeError)

      // eslint-disable-next-line no-undef
      const fn = someInArrayEquals(['SSB', 'QAK']).bind(someInArrayEquals, undefined)
      assert.throws(fn, TypeError)
    })

    it('returned function should identify a value present in given array', () => {
      const result = someInArrayEquals(['NNO', 1, 2, 3])('NNO')
      assert.isTrue(result)
    })
  })

  describe('someInArrayIsPrefixOf', () => {
    it('should return a function when given an argument', () => {
      const result = someInArrayIsPrefixOf(['NNO', 'NOB'])
      assert.isFunction(result)
    })

    it('returned function should return boolean when argument is any array', () => {
      const result = someInArrayIsPrefixOf([1, '2', {}])('NNO')
      assert.isBoolean(result)
    })

    it('returned function should throw when called with an argument of undefined', () => {
      assert.throws(someInArrayIsPrefixOf(['NOB', 'NNO']), TypeError)
    })

    it('should when called with an array return a function which can identify if any of the values in the array is a prefix of a given string', () => {
      const result = someInArrayIsPrefixOf(['NNO', 1, 2, 3])('NNOBELUGA')
      assert.isTrue(result)
    })
  })

  describe('inClosedInterval', () => {
    it('should return a function when given an argument', () => {
      const result = inClosedInterval([2, 5])
      assert.isFunction(result)
    })

    it('returned function should return boolean when argument is any array', () => {
      const result = inClosedInterval([1, 4])(5)
      assert.isBoolean(result)
    })

    it('returned function should return true when number is in given range', () => {
      const result = inClosedInterval([1, 4])(3)
      assert.isTrue(result)
    })
  })

  describe('matchRegexp', () => {
    it('should return a function when given an argument', () => {
      const result = matchRegexp('ab+cd')
      assert.isFunction(result)
    })

    it('returned function should return falsy when no match is found', () => {
      const result = matchRegexp('a+cd')('abcd')
      assert.isFalse(result)
    })

    it('returned function should throw when argument is not a string', () => {
      assert.throws(matchRegexp('ab+cd').bind(matchRegexp, true), TypeError)
    })

    it('returned function should return truthy when a matching regexp is given', () => {
      const result = matchRegexp('abc')('abcdefgh')
      assert.isTrue(result)
    })
  })

  describe('filterFn', () => {
    it('should return a function given the name of a filter', () => {
      const fn = filterFn('requestedUri')
      assert.isFunction(fn)
    })

    it('should return a predicateFn resolving true given a non-existent filter name', () => {
      const doc = {}
      const fn = filterFn('barbie')(doc)
      assert.isTrue(fn('whatever'))
      assert.isTrue(fn('yo mo'))
      assert.isTrue(fn(32))
    })
  })

  describe('filterToPredicate', () => {
    it('should return a function when given arguments', () => {
      const filter = filterSet.filters.find(_ => _.name === 'requestedUri')
      const fn = filterToPredicate(filter)
      assert.isFunction(fn)
    })

    it('returned function should return boolean when given an array argument', () => {
      const filter = filterSet.filters.find(_ => _.name === 'requestedUri')
      const fn = filterToPredicate(filter)
      const predicate = fn(data[0])
      assert.isBoolean(predicate)
    })

    it('predicate function should filter a selection as defined by the filter', () => {
      const filter = { name: 'language', value: ['NNO'] }
      const no = { language: 'BOR' }
      const yes = { language: 'NNO' }
      const selection = [no, yes]
      const result = selection.filter(filterToPredicate(filter))
      assert.deepEqual(result, [yes])
    })
  })

  describe('filtersToPredicates', () => {
    it('should return an array of functions', () => {
      assert.isArray(filtersToPredicates(filterSet.filters))
      filtersToPredicates(filterSet.filters).forEach(p => assert.isFunction(p))
    })

    it('should return an array of predicate functions', () => {
      filtersToPredicates(filterSet.filters)
        .map(p => p({ notPresent: false }))
        .forEach(bool => assert.isBoolean(bool))
    })
  })

  describe('filtersToPredicate', () => {
    it('should return a function given an array of filters', () => {
      assert.isArray(filterSet.filters)
      const result = filtersToPredicate(filterSet.filters)
      assert.isFunction(result)
    })

    it('should filter a selection of according to the rules (of language)', () => {
      const filter = { name: 'language', value: ['NNO'] }
      const no = { language: 'BOR' }
      const yes = { language: 'NNO' }
      const selection = [no, yes]
      const result = selection.filter(filtersToPredicate([filter]))
      assert.deepEqual(result, [yes])
    })

    it('should filter a selection according to the rules (of requestedUri)', () => {
      const no = { language: 'BOR', requestedUri: 'https://www.nb.no' }
      const yes = { language: 'NNO', requestedUri: 'https://nettarkivet.nb.no' }
      const selection = [no, yes]
      const filters = [
        { name: 'requestedUri', value: ['https://nettarkivet'] },
        { name: 'language', value: ['NNO'] }
      ]

      const result = selection.filter(filtersToPredicate(filters))
      assert.deepEqual(result, [yes])
    })

    it('should support exlusive filters', () => {
      const predicates = filtersToPredicate(filterSet.filters)
      const results = data.filter(predicates)

      const exclusiveFilters = filterSet.filters.filter(f => f.exclusive === true)
      exclusiveFilters.forEach(filter => {
        assert.isFalse(results.some(result => result[filter.field || filter.name].match(filter.value)))
      })
    })
  })

  describe('intervalize', () => {
    it('places the ids of filterSets into time intervals', () => {
      const [a, b, c, d, e, f, g, h, i] = intervalize(filterSets)

      const expected = {
        a: [a, b, c, d, e, f, g, h],
        b: [c, d],
        c: [e, f],
        d: [b, c, d, e, f, g],
        e: [d],
        f: [i]
      }
      Object.entries(expected).forEach(([id, haystack]) =>
        assert.isTrue(haystack.every(needle => needle.ids.includes(id)))
      )
    })

    it('handles single interval without bounds', () => {
      const sets = filterSets.slice(0, 1)
      const intervals = intervalize(sets)
      assert.equal(1, intervals.length)
      assert.equal(1, intervals[0].ids.length)
      assert.equal(sets[0].id, intervals[0].ids[0])
    })
  })
})
