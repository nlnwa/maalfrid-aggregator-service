/**
 *
 * @typedef {{
 *  ...languageCode: string
 * }} Metrics
 *
 * @typedef {{
 *   id: string,
 *   startTime: Date,
 *   endTime: Date,
 *   ...[meta]: *
 * }} LogEntry
 *
 * @typedef {{
 *   warcId: string,
 *   text: string
 * }} ExtractedText
 *
 * @typedef {{
 *   warcId: string,
 *   seedId: string,
 *   executionId: string,
 *   jobExecutionId: string,
 *   startTime: Date,
 *   endTime: Date,
 *   timeStamp: Date,
 *   size: number,
 *   wordCount: number,
 *   sentenceCount: number,
 *   longWordCount: number,
 *   lix: number,
 *   characterCount: number,
 *   requestedUri: string,
 *   discoveryPath: string,
 *   contentType: string
 * }} Aggregate
 *
 * @typedef {{
 *   name: string,
 *   [field]: string,
 *   [exclusive]: boolean,
 *   value: *
 * }} Filter
 *
 * @typedef {{
 *   id: string,
 *   seedId: string,
 *   validFrom: Date,
 *   validTo: Date,
 *   filters: Filter[]
 * }} FilterSet
 *
 * @typedef {{
 *   ids: string[],
 *   [from]: Date,
 *   [to]: Date
 * }} FilterInterval
 *
 * @typedef {{
 *   entityId: string,
 *   seedId: string,
 *   executionId: string,
 *   jobExecutionId: string,
 *   endTime: Date,
 *   statistic: {...any: {short: number, total: number}},
 * }} Statistic
 *
 * @typedef {function(*): boolean} Predicate
 *
 * @typedef {{
 *   code: number,
 *   message: string
 * }} AppError
 */
