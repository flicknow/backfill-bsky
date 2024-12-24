import { iterateAtpRepo } from '@atcute/car'
import { parse as parseTID } from '@atcute/tid'
import { XRPCError } from '@atcute/client'
import PQueue from 'p-queue'
import net from 'net'
import { parseArgs } from 'util'
import { type CommitMessage, ConnectionPool, exit_with_message, lineReaderSync, LOGGER, WrappedRPC } from './util.js'

export const writeWorkerAllocations = [[
    "app.bsky.feed.post",
    "chat.bsky.actor.declaration",
    "app.bsky.feed.postgate",
    "app.bsky.labeler.service",
    "app.bsky.feed.generator",
], [
    "app.bsky.feed.like",
    "app.bsky.actor.profile",
    "app.bsky.graph.list",
    "app.bsky.graph.block",
    "app.bsky.graph.starterpack",
], [
    "app.bsky.feed.threadgate",
    "app.bsky.feed.repost",
    "app.bsky.graph.follow",
    "app.bsky.graph.listitem",
    "app.bsky.graph.listblock",
]];

const collectionToWorkerIndex: Map<string, number> = new Map
writeWorkerAllocations.forEach((collections, i) => {
    collections.forEach(collection => {
        collectionToWorkerIndex.set(collection, i)
    })
})

const ACTIVE: Map<number, any> = new Map
process.on('SIGUSR1', () => {
    console.log({ 'queue size': QUEUE?.size })
    console.log(JSON.stringify(ACTIVE))
})

process.on('SIGINT', () => {
    console.log(`Interrupt!\n`)
    process.exit(0);
})

async function processDid(conns: net.Socket[], did: string, pds: string, signal: AbortSignal, id: number) {
    try {
        ACTIVE.set(id, { pds: pds })
        const rpc = new WrappedRPC(pds);
        const { data: repo } = await rpc.get("com.atproto.sync.getRepo", {
            signal: signal,
            params: { did: did as `did:${string}` },
        });
        ACTIVE.set(id, { did: did, pds: pds })

        let count = 0
        const now = Date.now();
        for await (const { record, rkey, collection, cid } of iterateAtpRepo(repo)) {
            const uri = `at://${did}/${collection}/${rkey}`;
            ACTIVE.set(id, { did: did, pds: pds, record: ++count })
            LOGGER.record([record])
            if (signal.aborted) {
                throw `timed out processing ${pds}${did}`
            }

            // This should be the date the AppView saw the record, but since we don't want the "archived post" label
            // to show for every post in social-app, we'll try our best to figure out when the record was actually created.
            // So first we try createdAt then parse the rkey; if either of those is in the future, we'll use now.
            let indexedAt: number =
                (!!record && typeof record === "object" && "createdAt" in record
                    && typeof record.createdAt === "string"
                    && new Date(record.createdAt).getTime()) || 0;
            if (!indexedAt || isNaN(indexedAt)) {
                try {
                    indexedAt = parseTID(rkey).timestamp;
                } catch {
                    indexedAt = now;
                }
            }
            if (indexedAt > now) indexedAt = now;

            const data = {
                uri,
                cid: cid.$link,
                timestamp: new Date(indexedAt).toISOString(),
                obj: record,
            };
            const commit = { type: "commit", collection, data } satisfies CommitMessage

            let index: number | undefined
            if (conns.length == 1) {
                index = 0
            } else {
                index = collectionToWorkerIndex.get(collection)
                if (index === undefined) {
                    console.log(`WARNING couldn't find index for ${collection}`)
                    index = 0
                }
            }

            ACTIVE.set(id, { did: did, pds: pds, writing: uri })
            await new Promise(resolve => {
                let payload = JSON.stringify(commit) + "\n"
                conns[index].write(payload, resolve)
            })
        }
    } catch (err) {
        if (err instanceof XRPCError) {
            switch (err.kind) {
                case 'InternalServerError':
                case 'NotFound':
                case 'RepoDeactivated':
                case 'RepoNotFound':
                case 'RepoTakendown':
                    console.log(`${did} (${pds}): ${err.kind}`)
                    return
                default:
            }
        } else if (err instanceof TypeError) {
            if (err.message == 'fetch failed') {
                console.log(`${did} (${pds}): ${err.message}`)
                return
            }
        }

        console.log({ err })
        throw err
    } finally {
        ACTIVE.delete(id)
    }
}

let QUEUE: PQueue | undefined = undefined

function getopts(argv: string[]) {
    return parseArgs({
        allowPositionals: true,
        args: argv,
        options: {
            'concurrency': {
                default: process.env.PRODUCER_CONCURRENCY ?? '16',
                type: 'string',
            },
            'processing-timeout': {
                default: process.env.PRODUCER_PROCESSING_TIMEOUT ?? '60',
                type: 'string',
            },
            'starting-line': {
                default: '0',
                type: 'string',
            },
            'ending-line': {
                default: '0',
                type: 'string',
            },
        },
    })
}

async function main(argv: string[]) {
    const { values: opts, positionals: [didPath, ...socketPaths] } = getopts(argv)

    LOGGER.start(parseInt(process.env.LOGGING_INTERVAL ?? '10'))

    if (!socketPaths.length) exit_with_message(`USAGE: ${process.argv[1]} DID-FILE SOCKET-PATHS...`)
    if ((socketPaths.length != 1) && (socketPaths.length != 3)) {
        exit_with_message('ERROR: 1 or 3 socket paths must be specified')
    }

    const pools = socketPaths.map(path => new ConnectionPool(path, parseInt(opts['concurrency'])))
    QUEUE = new PQueue({ concurrency: parseInt(opts['concurrency']) })
    QUEUE.on('error', error => {
        console.error({ 'queue error': error });
    });

    let iter = 0
    const timeout = parseInt(opts['processing-timeout'])
    const reader = lineReaderSync(didPath, parseInt(opts['starting-line']), parseInt(opts['ending-line']))
    for (const line of reader) {
        const [did, pds] = JSON.parse(line)
        if (pds.includes("blueski.social")) continue;

        await QUEUE.onSizeLessThan(QUEUE.concurrency)
        QUEUE.add(async () => {
            const id = iter++
            const conns = pools.map(pool => pool.connection())

            try {
                const signal = AbortSignal.timeout(timeout * 1000)
                await processDid(conns, did, pds, signal, id)
            } catch (err) {
                if (`${err}`.match(/timed out processing.*/)) return
                throw err
            } finally {
                pools.map((pool, i) => { pool.release(conns[i]) })
            }
        })
    }

    LOGGER.stop()
}
main(process.argv.slice(2))