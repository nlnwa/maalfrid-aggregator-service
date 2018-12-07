const config = require('./config')
const grpc = require('grpc')
const protoLoader = require('@grpc/proto-loader')
const packageDefinition = protoLoader.loadSync(config.languageService.protocol, {
  enums: String,
  defaults: true
})
const protocol = grpc.loadPackageDefinition(packageDefinition)
const address = config.languageService.host + ':' + config.languageService.port
const credentials = grpc.credentials.createInsecure()
const detector = new protocol.maalfrid.service.language.LanguageDetector(address, credentials)

function detectLanguage (text) {
  return new Promise((resolve, reject) => {
    detector.detectLanguage({ text: text }, (err, response) => {
      if (err) {
        return reject(err)
      }
      return resolve(response.languages)
    })
  })
}

module.exports = {
  detectLanguage
}
