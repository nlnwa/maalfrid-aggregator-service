const Promise = require('bluebird')

async function map (cursor, fn, options) {
  const iter = makeIterator(cursor)
  let done = false
  do {
    const rows = []
    do {
      let value
      ({ value, done } = await iter.next())
      if (!done) {
        rows.push(value)
      }
    } while (rows.length <= options.concurrency && !done)
    await Promise.map(rows, fn, options)
  } while (!done)
}

function makeIterator (cursor) {
  return {
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
    },
    [Symbol.iterator]: function () { return this }
  }
}

module.exports = {
  map
}
