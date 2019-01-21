const ERROR = {
  IN_PROGRESS: 1,
  FAILED_PRECONDITION: 2
}

function InProgressError (message) {
  const error = Error(message)
  error.code = ERROR.IN_PROGRESS
  return error
}

function FailedPreconditionError (message) {
  const error = Error(message)
  error.code = ERROR.IN_PROGRESS
  return error
}

module.exports = {
  ERROR,
  InProgressError,
  FailedPreconditionError
}
