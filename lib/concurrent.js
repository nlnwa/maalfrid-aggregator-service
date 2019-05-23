/**
 * map applies given async mapper function to values of cursor concurrently
 * until cursor is exhausted
 *
 * @param {RCursor} cursor - RethinkDB cursor
 * @param {function(*): *}fn - mapper function
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
 * @param {RCursor} cursor - rethinkDB cursor
 * @param {number} count - number of rows to collect before yielding
 * @returns {AsyncIterableIterator<Array>}
 */
async function * rowCollector (cursor, count = 1) {
  let rows = []
  for await (const value of cursor) {
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

module.exports = {
  rowCollector,
  map
}
