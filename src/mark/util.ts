import { toCIDLink, CIDLinkWrapper } from '@atcute/cbor';
import * as atcuteCID from '@atcute/cid';
import {
    type HeadersObject,
    simpleFetchHandler,
    XRPC,
    XRPCError,
    type XRPCRequestOptions,
    type XRPCResponse,
} from "@atcute/client";
import { BlobRef } from "@atproto/lexicon";
import * as fs from 'fs'
import { CID } from "multiformats/cid";
import net from 'net'
import * as ui8 from 'uint8arrays'

const backoffs = [1_000, 5_000, 15_000, 30_000, 60_000, 120_000, 300_000];
const sleep = (ms: number) => new Promise((resolve) => {
    setTimeout(resolve, ms)
});

async function processRatelimitHeaders(
    headers: HeadersObject,
    url: string,
    onRatelimit: (wait: number) => unknown,
) {
    const remainingHeader = headers["ratelimit-remaining"],
        resetHeader = headers["ratelimit-reset"];
    if (!remainingHeader || !resetHeader) return;

    const ratelimitRemaining = parseInt(remainingHeader);
    if (isNaN(ratelimitRemaining) || ratelimitRemaining <= 1) {
        const ratelimitReset = parseInt(resetHeader) * 1000;
        if (isNaN(ratelimitReset)) {
            console.error("ratelimit-reset header is not a number at url " + url);
        } else {
            const now = Date.now();
            const waitTime = ratelimitReset - now + 1000; // add 1s to be safe
            if (waitTime > 0) {
                console.log("Rate limited at " + url + ", waiting " + waitTime + "ms");
                await onRatelimit(waitTime);
            }
        }
    }
}

export class WrappedRPC extends XRPC {
    constructor(public service: string) {
        super({ handler: simpleFetchHandler({ service }) });
    }

    override async request(options: XRPCRequestOptions, attempt = 0): Promise<XRPCResponse> {
        const url = new URL("/xrpc/" + options.nsid, this.service).href;

        const request = async () => {
            const res = await super.request(options);
            await processRatelimitHeaders(res.headers, url, sleep);
            return res;
        };

        try {
            return await request();
        } catch (err) {
            if (attempt > 6) throw err;

            if (err instanceof XRPCError) {
                if (err.status === 429) {
                    await processRatelimitHeaders(err.headers, url, sleep);
                } else throw err;
            } else if (err instanceof TypeError) {
                console.warn(`fetch failed for ${url}, skipping`);
                throw err;
            } else {
                await sleep(backoffs[attempt] || 60000);
            }
            console.warn(`Retrying request to ${url}, on attempt ${attempt}`);
            return this.request(options, attempt + 1);
        }
    }
}

export type CommitData = { uri: string; cid: string; timestamp: string; obj: unknown };
export type CommitMessage = { type: "commit"; collection: string; data: CommitData };

export type JsonValue =
    | boolean
    | number
    | string
    | null
    | undefined
    | unknown
    | Array<JsonValue>
    | { [key: string]: JsonValue }

export type IpldValue =
    | JsonValue
    | CIDLinkWrapper
    | Uint8Array
    | Array<IpldValue>
    | { [key: string]: IpldValue }


export const jsonToIpld = (val: JsonValue): IpldValue => {
    // walk arrays
    if (Array.isArray(val)) {
        return val.map((item) => jsonToIpld(item))
    }
    // objects
    if (val && typeof val === 'object') {
        // check for dag json values
        if (typeof val['$link'] === 'string' && Object.keys(val).length === 1) {
            return toCIDLink(atcuteCID.parse(val['$link']))
        }
        if (typeof val['$bytes'] === 'string' && Object.keys(val).length === 1) {
            return ui8.fromString(val['$bytes'], 'base64')
        }
        // walk plain objects
        const toReturn = {}
        for (const key of Object.keys(val)) {
            toReturn[key] = jsonToIpld(val[key])
        }
        return toReturn
    }
    // pass through
    return val
}

export function convertBlobRefs(obj: unknown): unknown {
    if (!obj) return obj;
    if (Array.isArray(obj)) {
        for (let i = 0; i < obj.length; i++) {
            obj[i] = convertBlobRefs(obj[i]);
        }
    } else if (typeof obj === "object") {
        const record = obj as Record<string, any>;

        // weird-ish formulation but faster than for-in or Object.entries
        const keys = Object.keys(record);
        let i = keys.length;
        while (i--) {
            const key = keys[i];
            const value = record[key];
            if (typeof value === "object" && value !== null) {
                if (value.$type === "blob") {
                    try {
                        const cidLink = CID.parse(value.ref.$link);
                        record[key] = new BlobRef(cidLink, value.mimeType, value.size);
                    } catch {
                        console.warn(
                            `Failed to parse CID ${value.ref.$link}\nRecord: ${JSON.stringify(record)
                            }`,
                        );
                        return record;
                    }
                } else {
                    convertBlobRefs(value);
                }
            }
        }
    }

    return obj;
}

export function* lineReaderSync(path: string, startLine?: number, endLine?: number) {
    const contents = fs.readFileSync(path)
    const endByte = contents.length - 1

    let curr = 0
    let linesSeen = 0
    while (true) {
        let i = contents.indexOf(10, curr)
        if (i == -1) {
            return contents.subarray(curr).toString('utf8')
        }

        let line = contents.subarray(curr, i).toString('utf8')
        curr = i + 1
        linesSeen++

        if (startLine && (startLine > (linesSeen - 1))) {
            continue
        }
        if (endLine && (endLine <= (linesSeen - 1))) {
            return line
        }

        yield line
    }
}

let LOGGING_INTERVAL_ID: NodeJS.Timeout | undefined
let LOGGING_LAST_SEEN = 0
let LOGGING_TOTAL_SEEN = 0
export const LOGGER = {
    start: function (interval: number) {
        if (LOGGING_INTERVAL_ID !== undefined) {
            return
        }

        LOGGING_INTERVAL_ID = setInterval(() => {
            let total = LOGGING_TOTAL_SEEN
            let seen = LOGGING_TOTAL_SEEN - LOGGING_LAST_SEEN
            console.log(`> SEEN ${seen / interval} RECORDS/S`)
            LOGGING_LAST_SEEN = total
        }, interval * 1000)

    },
    stop: function () {
        if (LOGGING_INTERVAL_ID !== undefined) {
            return
        }
        clearInterval(LOGGING_INTERVAL_ID)
    },
    record: function (records: any[]) {
        LOGGING_TOTAL_SEEN += records.length
    },
}

export class ConnectionPool {
    public connections: {
        active: Set<net.Socket>,
        idle: Array<net.Socket>,
    }

    constructor(public path: string, public concurrency: number) {
        this.concurrency = concurrency
        this.connections = {
            active: new Set,
            idle: [],
        }

        for (let i = 0; i < concurrency; i++) {
            const conn = net.connect(path)
            conn.on('close', hadErr => {
                console.log({ 'socket close': hadErr })
            })
            conn.on('error', err => {
                console.log({ 'socket error': err })
            })
            conn.on('timeout', () => {
                console.log('socket timeout');
                conn.end();
            });

            this.connections.idle.push(conn)
        }
    }

    connection() {
        return this.connections.idle.pop()!
    }

    release(conn: net.Socket) {
        this.connections.active.delete(conn)
        this.connections.idle.push(conn)
    }
}

export function exit_with_message(msg: string) {
    console.log(msg)
    process.exit(1)
}