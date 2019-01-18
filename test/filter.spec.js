const { describe, it, before } = require('mocha')
const { assert } = require('chai')

const reqlMock = {
  expr: (_) => _,
  add: (a, b) => new RegExp(a.concat(b)),
  and: (a, b) => a && b
}

const {
  someInArrayEquals,
  someInArrayIsPrefixOf,
  inClosedInterval,
  matchRegexp,
  prefixOf,
  filterFn,
  makePredicate,
  filterToPredicate
} = require('../lib/filter')(reqlMock, console)

const filters = require('./filter').filter
const selection = require('./filter.data')

describe('lib/filter.js', () => {
  before('monkeypatch built-ins to emulate reql object functions', () => {
    // eslint-disable-next-line no-extend-native
    Array.prototype.contains = Array.prototype.some
    // eslint-disable-next-line no-extend-native
    String.prototype.eq = function (str) { return this.toString() === str }
    // eslint-disable-next-line no-extend-native
    Object.prototype.getField = function getField (field) {
      if (!this.hasOwnProperty(field)) {
        throw new Error('No attribute ' + field + ' in object: ' + this)
      }
      return this[field]
    }
    // eslint-disable-next-line no-extend-native
    Number.prototype.gt = function (number) { return this.valueOf() > number }
    // eslint-disable-next-line no-extend-native
    Number.prototype.gte = function (number) { return this.valueOf() >= number }
    // eslint-disable-next-line no-extend-native
    Number.prototype.lt = function (number) { return this.valueOf() < number }
    // eslint-disable-next-line no-extend-native
    Number.prototype.lte = function (number) { return this.valueOf() <= number }
    // eslint-disable-next-line no-extend-native
    Boolean.prototype.and = function (val) { return this.valueOf() && val }
    // eslint-disable-next-line no-extend-native
    Boolean.prototype.not = function () { return !this.valueOf() }
  })

  describe('spec', () => {
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

    it('getField should be an object method returning named property of object', () => {
      const expected = 2
      const obj = { expected }
      const actual = obj.getField('expected')
      assert.equal(expected, actual)
    })

    it('Boolean.prototype.and', () => {
      assert.isFalse(false.and(true))
      assert.isFalse(true.and(false))
      assert.isTrue(true.and(true))
    })
  })

  describe('prefixOf', () => {
    it('should return a function when given an argument', () => {
      assert.isFunction(prefixOf('hello'))
    })

    it('returned function should return null when argument of function is not a prefix', () => {
      assert.isNull(prefixOf('hello')('go'))
    })

    it('returned function should return an array when argument of function is a prefix', () => {
      const result = prefixOf('golang')('go')

      assert.isArray(result)
      assert.equal(result[0], 'go')
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

    it('returned function should work', () => {
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

    it('returned function should throw when argument is undefined', () => {
      assert.throws(someInArrayIsPrefixOf(['NOB', 'NNO']), TypeError)
    })

    it('returned function should return true when there is some array element who is a prefix of stored string', () => {
      const result = someInArrayIsPrefixOf(['NNOB', 1, 2, 3])('NNOBELUGA')
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
      const result = matchRegexp(/ab+cd/)
      assert.isFunction(result)
    })

    it('returned function should return boolean when argument is a string', () => {
      const result = matchRegexp(/ab+cd/)('abcd')
      assert.isBoolean(result)
    })

    it('returned function should throw when argument is not a string', () => {
      assert.throws(matchRegexp(/ab+cd/).bind(matchRegexp, true), TypeError)
    })

    it('returned function should return true when a matching regexp is given', () => {
      const result = matchRegexp(/abc/)('abcdefgh')
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

  describe('makePredicate', () => {
    it('should return a function when given arguments', () => {
      const filter = filters.find(_ => _.name === 'requestedUri')
      const fn = makePredicate(filter)
      assert.isFunction(fn)
    })

    it('returned function should return boolean when given an array argument', () => {
      const filter = filters.find(_ => _.name === 'requestedUri')
      const predicateFn = makePredicate(filter)
      const predicate = predicateFn(selection[0])
      assert.isBoolean(predicate)
    })
  })

  describe('filterToPredicate', () => {
    it('should return a function given an array of filters', () => {
      assert.isArray(filters)
      const result = filterToPredicate(filters)
      assert.isFunction(result)
    })

    it('should filter a selection of according to the rules (of language)', () => {
      const no = { language: 'BOR' }
      const yes = { language: 'NNO' }
      const selection = [no, yes]

      const result = selection.filter(filterToPredicate([{ name: 'language', value: ['NNO'] }]))
      assert.deepEqual(result, [yes])
    })

    it('should filter a selection of according to the rules (of requestedUri)', () => {
      const no = { language: 'BOR', requestedUri: 'https://www.nb.no' }
      const yes = { language: 'NNO', requestedUri: 'https://nettarkivet.nb.no' }
      const selection = [no, yes]
      const filters = [
        { name: 'requestedUri', value: ['https://nettarkivet'] },
        { name: 'language', value: ['NNO'] }
      ]

      const result = selection.filter(filterToPredicate(filters))
      assert.deepEqual(result, [yes])
    })

    it('should support exlusive filters', () => {
      const predicates = filterToPredicate(filters)
      const result = selection.filter(predicates)
      assert.isTrue(result.length === 7)
      const exlusiveFilter = filters.find(_ => _.exlusive === true)
      assert.isTrue(result.find(_ => _.requestedUri.match(exlusiveFilter.value)) === undefined)
    })
  })
})
