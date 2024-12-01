'use strict'

const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3')

const CONFLICT_ERROR_CODES = [
  'ConditionalRequestConflict',
  'PreconditionFailed'
]

class S3Adapter {
  constructor (config) {
    this._client = new S3Client({
      endpoint: config.endpoint || null,
      region: config.region,
      forcePathStyle: false,
      credentials: {
        accessKeyId: config.accessKeyId,
        secretAccessKey: config.secretAccessKey
      }
    })

    this._bucket = config.bucket
  }

  async read (id) {
    try {
      let cmd = new GetObjectCommand({ Bucket: this._bucket, Key: id })
      let response = await this._client.send(cmd)

      return {
        value: await response.Body.transformToString(),
        rev: response.ETag
      }

    } catch (error) {
      if (error.Code === 'NoSuchKey') {
        return null
      } else {
        throw error
      }
    }
  }

  async write (id, value, rev = null) {
    let params = { Bucket: this._bucket, Key: id, Body: value }

    if (rev === null) {
      params.IfNoneMatch = '*'
    } else {
      params.IfMatch = rev
    }

    try {
      let cmd = new PutObjectCommand(params)
      let response = await this._client.send(cmd)
      return { rev: response.ETag }

    } catch (error) {
      if (CONFLICT_ERROR_CODES.includes(error.Code)) {
        throw new ConflictError(error.message, { cause: error })
      } else {
        throw error
      }
    }
  }
}

class ConflictError extends Error {
  constructor (message, { cause }) {
    super(message, { cause })
    this.code = 'ERR_CONFLICT'
    this.name = 'ConflictError'
  }
}

module.exports = S3Adapter
