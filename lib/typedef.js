/**
 * @typedef {{
 *   next: function(): *
 * }} Cursor
 *
 * @typedef {{
 *   seconds: number,
 *   nanos: number
 * }} Timestamp
 *
 * @typedef {{
 *  ...languageCode: string
 * }} Metrics
 *
 * @typedef {{
 *   id: string,
 *   startTime: Date,
 *   endTime: Date,
 *   [seedId]: string,
 *   [lowerBound]: Date,
 *   [upperBound]: Date,
 *   ...[meta]: *
 * }} LogEntry
 *
 * @typedef {{

 *   deleted: number,
 *   errors: number,
 *   inserted: number,
 *   [replaced]: number,
 *   [skipped]: number,
 *   [unchanged]: number,
 *   [generated_keys]: string[],
 *   [warnings]: string,
 *   [changes]: {new_val: *, old_val: *}[],
 *   [first_error]: string,
 * }} Changes
 *
 *
 * @typedef {{
 *   filter: function(*): Selection,
 *   group: function(field: string): *,
 *   pluck: function(string): Selection
 * }} Selection
 *
 *
 * @typedef {{
 *   db: function(string): Database,
 *   branch: function(test: *, true_action: *, ...),
 *   now: function(): Date, desc: function(*): *,
 *   args: function(*): *,
 *   object: function(*, *): *,
 *   row: function(*): *,
 *   minval: Date,
 *   maxval: Date,
 *   add: function(number|string, number|string): number|string,
 *   expr: function(*): Selection,
 *   not: function(bool: boolean): boolean,
 *   and: function(bool: boolean, bool: boolean): boolean,
 *   epochTime: function(number): Date
 * } | *} ReQL
 *
 * @typedef {{
 *   table: function(string): Selection,
 * }} Database
 *
 * @typedef {{
 *   warcId: string,
 *   text: string
 * }} ExtractedText
 *
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
 *
 * @typedef {{
 *   name: string,
 *   [field]: string,
 *   [exclusive]: boolean,
 *   value: *
 * }} Filter
 *
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
 *
 * @typedef {function(*): boolean} Predicate
 *
 * @typedef {{
 *   code: number,
 *   message: string
 * }} AppError
 */
