const grpc = require('grpc')

/**
 * @enum {number}
 */
const ERROR = {
  IN_PROGRESS: 1,
  NOT_FOUND: 2
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

function NotFoundError (message) {
  /**
   * @type {AppError|Error}
   */
  const error = Error(message)
  error.code = ERROR.NOT_FOUND
  return error
}

/**
 * Convert error to gRPC error
 *
 * @param {AppError|Error} error
 * @returns {AppError|Error}
 */
function grpcError (error) {
  switch (error.code) {
    case ERROR.NOT_FOUND:
      error.code = grpc.status.NOT_FOUND
      break
    case ERROR.IN_PROGRESS:
      error.code = grpc.status.UNAVAILABLE
      break
    default:
      error.code = grpc.status.INTERNAL
      break
  }
  return error
}

module.exports = {
  ERROR,
  grpcError,
  InProgressError,
  NotFoundError
}
