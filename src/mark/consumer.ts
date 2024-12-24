import { IdResolver, MemoryCache } from '@atproto/identity'
import * as bsky from "@futuristick/atproto-bsky"
import { AtUri } from '@atproto/syntax'
import * as fs from 'fs'
import { CID } from 'multiformats/cid'
import net from 'net'
import pg from 'pg'
import readline from 'readline'
import { parseArgs } from 'util'
import { type CommitMessage, convertBlobRefs, exit_with_message, jsonToIpld, LOGGER } from './util.js'

type IndexingService = bsky.RepoSubscription["indexingSvc"]
type ToInsertCommit = { uri: AtUri; cid: CID; timestamp: string; obj: unknown };

function* genFollowRow() {
    const base = {
        uri: "at://did:plc:22223gqtyfos6p6eaz2d7tmk/app.bsky.graph.follow/3laud6hejel25",
        cid: "bafyreidup7jmjmyublol7oewrwfb3mxmcj2pbxpcv4xizos4ldnz77v7py",
        host: "did:plc:22223gqtyfos6p6eaz2d7tmk",
        subject: "did:plc:z72i7hdynmk6r22z27h6tvur",
        createdAt: new Date().getTime(),
        timestamp: new Date().getTime(),
    }

    let iter = 0
    while (true) {
        iter++
        yield [
            `${base.uri}${iter}`,
            `${base.cid}${iter}`,
            `${base.host}${iter}`,
            `${base.subject}${iter}`,
            new Date(base.createdAt + iter).toISOString(),
            new Date(base.timestamp + iter).toISOString(),
        ]
    }
}

function getopts(argv: string[]) {
    return parseArgs({
        allowPositionals: true,
        args: argv,
        options: {
            'batch-size': {
                default: process.env.CONSUMER_BATCH_SIZE ?? '1000',
                type: 'string',
            },
            'transaction-size': {
                default: process.env.CONSUMER_TRANSACTION_SIZE ?? '1000000',
                type: 'string',
            },
            'pg-native': {
                default: !!process.env.CONSUMER_PG_NATIVE,
                type: 'boolean',
            },
            'max-db-connections': {
                default: process.env.CONSUMER_MAX_DB_CONNECTIONS ?? '1',
                type: 'string',
            },
            'db-test': {
                default: '',
                type: 'string',
            }
        },
    })
}

process.on('SIGINT', () => {
    console.log(`Interrupt!\n`)
    process.exit(0);
})


let CACHED_SQL_STATEMENT = ''
const DB_TESTS = {
    'with-params': async function (db: bsky.Database, batchSize: number, txnSize: number) {
        LOGGER.start(parseInt(process.env.LOGGING_INTERVAL ?? '10'))
        const conn = await db.pool.connect()
        conn.query('SET synchronous_commit = off')
        conn.query('BEGIN')

        let currTxnSize = 0
        let batch: Array<Array<string>> = []
        for (const row of genFollowRow()) {
            batch.push(row)
            if (batch.length >= batchSize) {
                if (!CACHED_SQL_STATEMENT) {
                    CACHED_SQL_STATEMENT = `INSERT INTO follow ("uri", "cid", "creator", "subjectDid", "createdAt", "indexedAt") VALUES `
                    CACHED_SQL_STATEMENT += batch
                        .map((_, i) => {
                            const j = i * 6
                            return `($${j + 1}, $${j + 2}, $${j + 3}, $${j + 4}, $${j + 5}, $${j + 6})`
                        })
                        .join(',')
                    CACHED_SQL_STATEMENT += ' ON CONFLICT DO NOTHING'
                }

                await conn.query({
                    //name: `insert-${batch.length}-follows`,
                    text: CACHED_SQL_STATEMENT,
                    values: batch.flat(),
                })
                LOGGER.record(batch)

                currTxnSize += batch.length
                batch = []
                if (currTxnSize >= txnSize) {
                    await conn.query('COMMIT')
                    await conn.query('BEGIN')
                    currTxnSize = 0
                }
            }
        }
    },
    'with-prepared': async function (db: bsky.Database, batchSize: number, txnSize: number) {
        LOGGER.start(parseInt(process.env.LOGGING_INTERVAL ?? '10'))
        const conn = await db.pool.connect()
        conn.query('SET synchronous_commit = off')
        conn.query('BEGIN')

        let currTxnSize = 0
        let batch: Array<Array<string>> = []
        for (const row of genFollowRow()) {
            batch.push(row)
            if (batch.length >= batchSize) {
                if (!CACHED_SQL_STATEMENT) {
                    CACHED_SQL_STATEMENT = `INSERT INTO follow ("uri", "cid", "creator", "subjectDid", "createdAt", "indexedAt") VALUES `
                    CACHED_SQL_STATEMENT += batch
                        .map((_, i) => {
                            const j = i * 6
                            return `($${j + 1}, $${j + 2}, $${j + 3}, $${j + 4}, $${j + 5}, $${j + 6})`
                        })
                        .join(',')
                    CACHED_SQL_STATEMENT += ' ON CONFLICT DO NOTHING'
                }

                await conn.query({
                    name: `insert-${batch.length}-follows`,
                    text: CACHED_SQL_STATEMENT,
                    values: batch.flat(),
                })
                LOGGER.record(batch)

                currTxnSize += batch.length
                batch = []
                if (currTxnSize >= txnSize) {
                    await conn.query('COMMIT')
                    await conn.query('BEGIN')
                    currTxnSize = 0
                }
            }
        }
    },
    'no-params': async function (db: bsky.Database, batchSize: number, txnSize: number) {
        LOGGER.start(parseInt(process.env.LOGGING_INTERVAL ?? '10'))
        const conn = await db.pool.connect()
        conn.query('SET synchronous_commit = off')
        conn.query('BEGIN')

        let currTxnSize = 0
        let batch: Array<Array<string>> = []
        for (const row of genFollowRow()) {
            batch.push(row)
            if (batch.length >= batchSize) {
                let insert = `INSERT INTO follow ("uri", "cid", "creator", "subjectDid", "createdAt", "indexedAt") VALUES `
                insert += batch
                    .map((row) => {
                        return `('${row[0]}','${row[1]}','${row[2]}','${row[3]}','${row[4]}','${row[5]}')`
                    })
                    .join(',')
                insert += ' ON CONFLICT DO NOTHING'

                await conn.query({
                    text: insert,
                })
                LOGGER.record(batch)

                currTxnSize += batch.length
                batch = []
                if (currTxnSize >= txnSize) {
                    await conn.query('COMMIT')
                    await conn.query('BEGIN')
                    currTxnSize = 0
                }
            }
        }
    },
}
type DB_TEST_KEY = keyof (typeof DB_TESTS)


async function processBatches(indexingSvc: IndexingService, batches: Map<string, ToInsertCommit[]>) {
    const toInsert = new Map(batches)
    for (const key of toInsert.keys()) {
        batches.set(key, [])
    }

    try {
        await indexingSvc.indexRecordsBulkAcrossCollections(toInsert)

        for (const batch of toInsert.values()) {
            LOGGER.record(batch)
        }
    } catch (err) {
        const collections = Array.from(toInsert.keys())
        console.error(`Error processing queue for ${collections.join(", ")}`, err);
    }
}

async function main(argv: string[]) {
    const { values: opts, positionals: [socketPath] } = getopts(argv)

    const pool = new (opts['pg-native'] ? pg.native!.Pool : pg.Pool)({
        connectionString: process.env.BSKY_DB_POSTGRES_URL!,
        max: parseInt(opts['max-db-connections']),
    })

    const db = new bsky.Database({
        url: process.env.BSKY_DB_POSTGRES_URL!,
        schema: process.env.BSKY_DB_POSTGRES_SCHEMA!,
        poolSize: parseInt(opts['max-db-connections']),
        pool: pool,
    });

    const idResolver = new IdResolver({
        plcUrl: process.env.BSKY_DID_PLC_URL,
        didCache: new MemoryCache(),
    });

    const { indexingSvc } = new bsky.RepoSubscription({
        service: process.env.BSKY_REPO_PROVIDER!,
        db,
        idResolver,
    });

    const batchSize = parseInt(opts['batch-size'])
    const txnSize = parseInt(opts['transaction-size'])
    if (opts['db-test']) {
        if (opts['db-test'] in DB_TESTS) {
            const testName = opts['db-test'] as DB_TEST_KEY
            return await DB_TESTS[testName](db, batchSize, txnSize)
        } else {
            const tests = Object.keys(DB_TESTS)
            exit_with_message(
                `unrecognized --db-test "${opts['db-test']}". must be one of:\n`
                + tests.map(key => `- ${key}`).join("\n")
            )
        }
    }

    if (!socketPath) exit_with_message(`USAGE: ${process.argv[1]} SOCKET-PATH`)
    if (fs.existsSync(socketPath)) {
        fs.unlinkSync(socketPath)
    }

    let batches: Map<string, Array<ToInsertCommit>> = new Map()
    const server = net.createServer().listen(socketPath)
    server.on('connection', async listener => {
        LOGGER.start(parseInt(process.env.LOGGING_INTERVAL ?? '10'))

        const reader = readline.createInterface({
            input: listener
        })
        listener.on('error', err => {
            console.log({ 'listener error': err })
        })

        for await (const line of reader) {
            const msg: CommitMessage = JSON.parse(line)
            if (msg.type !== "commit") throw new Error(`Invalid message type ${msg.type}`);

            const { uri, cid, timestamp, obj } = msg.data;
            if (!uri || !cid || !timestamp || !obj) {
                throw new Error(`Invalid commit data ${JSON.stringify(msg.data)}`);
            }

            const data = jsonToIpld(obj);
            convertBlobRefs(obj)

            const collection = msg.collection
            let batch: Array<ToInsertCommit> | undefined = batches.get(collection)
            if (batch === undefined) {
                batch = []
                batches.set(collection, batch)
            }
            batch.push({ uri: new AtUri(uri), cid: CID.parse(cid), timestamp, obj })

            if (batch.length >= batchSize) {
                await processBatches(indexingSvc, batches)
            }
        }

        if (Array.from(batches.values()).reduce((size, batch) => { return batch.length + size }, 0)) {
            await processBatches(indexingSvc, batches)
        }

    })
    server.on('error', err => {
        console.log({ 'server error': err })
    })
}
main(process.argv.slice(2))