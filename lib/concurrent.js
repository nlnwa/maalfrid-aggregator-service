/**
 * map applies given async mapper function to values of cursor concurrently
 * until cursor is exhausted
 *
 * @param cursor - RethinkDB cursor
 * @param fn - mapper function
 * @param concurrency - number of cursor values to apply fn to concurrently
 * @returns {Promise<void>}
 */
async function map (cursor, fn, concurrency) {
  for await (const rows of rowCollector(cursor, concurrency)) {
    await Promise.all(rows.map(fn))
  }
}

/**
 * rowCollector is a helper function to collect a number of rows from async iterator before yielding array of the rows
 *
 * @param cursor - rethinkDB cursor
 * @param count - number of rows to collect before yielding
 * @returns {AsyncIterableIterator<Array>}
 */
async function * rowCollector (cursor, count = 1) {
  let rows = []
  for await (const value of makeAsyncIterator(cursor)) {
    rows.push(value)
    if (rows.length >= count) {
      yield rows
      rows = []
    }
  }
  if (rows.length > 0) {
    yield rows
  }
}

/**
 *  Make async iterator from RethinkDB cursor
 *
 * @param cursor
 * @returns {{[Symbol.asyncIterator]: (function(): {next: (function(): {done: boolean, value: any})})}}
 */
function makeAsyncIterator (cursor) {
  const iterator = {
    next: async function () {
      const result = { done: false, value: undefined }
      try {
        result.value = await cursor.next()
      } catch (err) {
        if (err.name === 'ReqlDriverError' && err.message === 'No more rows in the cursor.') {
          result.done = true
        } else {
          throw err
        }
      }
      return result
    }
  }
  return { [Symbol.asyncIterator]: () => iterator }
}

module.exports = {
  rowCollector,
  makeAsyncIterator,
  map
}
