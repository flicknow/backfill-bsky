#!/usr/bin/env ts-node
import * as bsky from "@futuristick/atproto-bsky"
import { parseArgs } from 'util'

function getopts(argv: string[]) {
  return parseArgs({
    allowPositionals: true,
    args: argv,
    options: {
      'drop-follow-indexes': {
        default: false,
        type: 'boolean',
      },
      'recreate-schema': {
        default: false,
        type: 'boolean',
      },
    },
  })
}

async function main(argv: string[]) {
  const { values: opts } = getopts(argv)
  const url = process.env.BSKY_DB_POSTGRES_URL!
  const schema = process.env.BSKY_DB_POSTGRES_SCHEMA! ?? 'public'

  const db = new bsky.Database({
    schema: schema,
    url: url,
    poolSize: 1,
  })

  if (opts['recreate-schema']) {
    await db.db.schema.dropSchema(schema).ifExists().cascade().execute()
  }

  await db.migrateToLatestOrThrow()

  if (opts['drop-follow-indexes']) {
    await db.db.schema.dropIndex('follow_creator_cursor_idx').ifExists().execute()
    await db.db.schema.dropIndex('follow_subject_cursor_idx').ifExists().execute()
    await db.db.schema.alterTable('follow').dropConstraint('follow_unique_subject').ifExists().execute()
  }
}
main(process.argv.slice(2))