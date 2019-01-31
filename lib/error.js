/**
 * @enum
 * @type {{
 *   IN_PROGRESS: number,
 *   FAILED_PRECONDITION: number
 * }}
 */
const ERROR = {
  IN_PROGRESS: 1,
  FAILED_PRECONDITION: 2
}

function InProgressError (message) {
  /**
   *
   * @type {AppError|Error}
   */
  const error = Error(message)
  error.code = ERROR.IN_PROGRESS
  return error
}

function FailedPreconditionError (message) {
  /**
   * @type {AppError|Error}
   */
  const error = Error(message)
  error.code = ERROR.IN_PROGRESS
  return error
}

module.exports = {
  ERROR,
  InProgressError,
  FailedPreconditionError
}
