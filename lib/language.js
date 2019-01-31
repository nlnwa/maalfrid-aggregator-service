const config = require('./config')
const grpc = require('grpc')
const protoLoader = require('@grpc/proto-loader')
const packageDefinition = protoLoader.loadSync(config.languageService.protocol, {
  enums: String,
  defaults: true
})
/**
 * @type {{language: {LanguageDetector: function(address: string, credentials: *)}}}
 */
const service = grpc.loadPackageDefinition(packageDefinition).maalfrid.service
const address = config.languageService.host + ':' + config.languageService.port
const credentials = grpc.credentials.createInsecure()
const detector = new service.language.LanguageDetector(address, credentials)

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
