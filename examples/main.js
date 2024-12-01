'use strict'

require('dotenv').config()

const Store = require('@vault-db/core/lib/store')
const S3Adapter = require('../lib/s3_adapter')

async function main () {
  let adapter = new S3Adapter({
    endpoint: process.env.S3_ENDPOINT,
    region: process.env.S3_REGION,
    bucket: process.env.S3_BUCKET,
    accessKeyId: process.env.S3_ACCESS_KEY_ID,
    secretAccessKey: process.env.S3_SECRET_ACCESS_KEY
  })

  let store = await Store.create(adapter, {
    key: { password: 'test' },
    shards: { n: 4 }
  })

  await Promise.all([
    store.update('/doc', (doc) => ({ ...doc, a: 1 })),
    store.update('/doc', (doc) => ({ ...doc, b: 2 })),
    store.update('/doc', (doc) => ({ ...doc, c: 3 }))
  ])

  let doc = await store.get('/doc')

  console.log({ doc })
  // -> { a: 1, b: 2, c: 3 }
}

main()
