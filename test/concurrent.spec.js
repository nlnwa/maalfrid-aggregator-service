const { describe, it } = require('mocha')
const { assert } = require('chai')

const { makeAsyncIterator, rowCollector, map } = require('../lib/concurrent')

function isAsyncIterator (iterator) {
  return iterator &&
    typeof iterator[Symbol.asyncIterator] === 'function' &&
    typeof iterator[Symbol.asyncIterator]().next === 'function'
}

const getCursorMock = ({ shouldThrow = false, cursorSize = 10 } = {}) => ({
  count: 0,
  next: function () {
    if (this.count >= cursorSize) {
      if (shouldThrow) {
        throw Error('fail')
      } else {
        const err = Error('No more rows in the cursor.')
        err.name = 'ReqlDriverError'
        throw err
      }
    }
    this.count++
    return Promise.resolve({ count: this.count })
  }
})

describe('lib/concurrent.js', () => {
  describe('makeAsyncIterator (of RethinkDB cursor)', async () => {
    it('returns an async iterator', () => {
      assert.isTrue(isAsyncIterator(makeAsyncIterator(getCursorMock())))
    })

    it('the returned async iterator iterates until exhaustion', async () => {
      const cursorSize = 20
      const asyncIterator = makeAsyncIterator(getCursorMock({ cursorSize }))
      let count = 0
      for await (const value of asyncIterator) {
        assert.isOk(value)
        count++
      }
      assert.equal(count, cursorSize)
    })

    it('is possible to catch errors thrown by iterable', async () => {
      const cursorSize = 10
      const asyncIterator = makeAsyncIterator(getCursorMock({ shouldThrow: true, cursorSize }))
      let count = 0
      try {
        for await (const value of asyncIterator) {
          assert.isOk(value)
          count++
        }
        assert.fail('should throw')
      } catch (err) {}
      assert.equal(count, cursorSize)
    })
  })

  describe('rowCollector', () => {
    it('should yield an array containing specified number of elements or the final residual count', async () => {
      const cursorSize = 25
      const concurrency = 10
      for await (const rows of rowCollector(getCursorMock({ cursorSize }), concurrency)) {
        assert.oneOf(rows.length, [concurrency, cursorSize % concurrency])
      }
    })
  })

  describe('map', () => {
    it('should apply and await async mapper function concurrently to values of cursor until exhaustion', async () => {
      const cursorSize = 100
      const concurrency = Math.ceil(Math.random() * 100)
      let count = 0
      const mapper = async (row) => {
        count++
        return Promise.resolve(row.count)
      }
      await map(getCursorMock({ cursorSize }), mapper, concurrency)
      assert.equal(count, cursorSize)
    })
  })
})
