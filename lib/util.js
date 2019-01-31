/**
 * Convert google.protobuf.Timestamp to javascript Date
 *
 * @param {Timestamp} timestamp
 * @returns {Date} date
 */
function timestampToDate (timestamp) {
  if (!timestamp) {
    return null
  }
  return new Date(1e3 * timestamp.seconds + 1e6 * timestamp.nanos)
}

module.exports = {
  timestampToDate
}
