const { describe, it } = require('mocha')
const { assert } = require('chai')
const { Readable } = require('stream')
const { rowCollector, map } = require('../lib/concurrent')

const getCursorMock = ({ cursorSize = 10 } = {}) => {
  let count = cursorSize
  return new Readable({
    objectMode: true,
    read (size) {
      if (count > 0) {
        count--
        return this.push({ count })
      } else {
        return this.push(null)
      }
    }
  })
}

describe('lib/concurrent.js', () => {
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
