/**
 * @type {string}
 */
let LOGGER

const LEVEL = {
  TRACE: 0,
  DEBUG: 1,
  INFO: 2,
  WARN: 3,
  ERROR: 4,
  0: 'TRACE',
  1: 'DEBUG',
  2: 'INFO',
  3: 'WARN',
  4: 'ERROR'
}

const logFn = {
  [LEVEL.TRACE]: console.log,
  [LEVEL.DEBUG]: console.log,
  [LEVEL.INFO]: console.log,
  [LEVEL.WARN]: console.warn,
  [LEVEL.ERROR]: console.error
}

const configLevel = (process.env.LOG_LEVEL || '').toUpperCase()
const runLevel = LEVEL[configLevel] || LEVEL.INFO

/**
 * Log messages using level
 *
 * @param {number} level
 * @param {...string} messages
 */
function log (level, ...messages) {
  if (runLevel > level) return
  const args = [new Date().toISOString(), '[node]', LEVEL[level], LOGGER, '-', ...messages]
  logFn[level](...args)
}

/**
 * Create logger method for a level
 *
 * @param {number} level
 * @return {function(...string): void}
 */
const createLogger = (level) => (...args) => log(level, ...args)

/**
 * @param {string} logger name
 * @return {Object} an object with one logger method per log level
 */
module.exports = (logger) => {
  LOGGER = logger || ''
  return {
    trace: createLogger(LEVEL.TRACE),
    debug: createLogger(LEVEL.DEBUG),
    info: createLogger(LEVEL.INFO),
    warn: createLogger(LEVEL.WARN),
    error: createLogger(LEVEL.ERROR)
  }
}
