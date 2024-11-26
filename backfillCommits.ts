import { iterateAtpRepo } from "@atcute/car";
import { CredentialManager, type HeadersObject, XRPC, XRPCError } from "@atcute/client";
import { parse as parseTID } from "@atcute/tid";
import { sortFile } from "large-sort";
import * as fs from "node:fs";

export interface BackfillLine {
	action: "create";
	timestamp: number;
	uri: string;
	cid: string;
	record: unknown;
}

async function main() {
	const ws = fs.createWriteStream("backfill-unsorted.jsonl");

	let seenDids: Record<string, Record<string, boolean>>;
	try {
		seenDids = JSON.parse(fs.readFileSync("seen-dids.json", "utf-8"));
	} catch {
		seenDids = {};
	}

	setInterval(() => {
		fs.writeFileSync("seen-dids.json", JSON.stringify(seenDids));
	}, 30_000);

	const onFinish = () => {
		ws.close();
		fs.writeFileSync("seen-dids.json", JSON.stringify(seenDids));
	};

	process.on("SIGINT", onFinish);
	process.on("SIGTERM", onFinish);
	process.on("exit", onFinish);

	const pdses = await getPdses();

	await Promise.allSettled(pdses.map(async (pds) => {
		try {
			seenDids[pds] ??= {};
			for await (const did of listRepos(pds)) {
				if (seenDids[pds][did]) continue;
				try {
					const repo = await getRepo(pds, did);
					if (!repo) continue;
					for await (const { record, rkey, collection, cid } of iterateAtpRepo(repo)) {
						const uri = `at://${did}/${collection}/${rkey}`;
						try {
							const { timestamp } = parseTID(rkey);
							const line: BackfillLine = {
								action: "create",
								timestamp,
								uri,
								cid: cid.$link,
								record,
							};
							ws.write(JSON.stringify(line) + "\n");
						} catch {
							console.warn(
								`Skipping record ${uri} for pds ${pds} because of invalid rkey`,
							);
						}
					}
					seenDids[pds][did] = true;
				} catch (err) {
					console.warn(`Skipping repo ${did} for pds ${pds}: ${err}`);
				}
			}
			return pds;
		} catch (err) {
			console.warn(`Skipping pds ${pds}: ${err}`);
		}
	}));

	onFinish();

	await sortFile<BackfillLine>(
		"backfill-unsorted.jsonl",
		"backfill-sorted.jsonl",
		(str) => JSON.parse(str),
		(line) => JSON.stringify(line),
		(a, b) => a.timestamp > b.timestamp ? 1 : -1,
	);

	console.log(`Done sorting backfill data!
  
  Ensure the AppView is running, then run backfill-commits.ts to backfill the AppView.
  Keep buffer-relay.ts running to receive live events until this is done. Then run backfill-buffer.ts to fill in the buffer, then restart the AppView to begin from cursor: 0.`);
}

async function getRepo(pds: string, did: `did:${string}`) {
	const rpc = new XRPC({ handler: new CredentialManager({ service: pds }) });

	try {
		const { data, headers } = await rpc.get("com.atproto.sync.getRepo", { params: { did } });
		await parseRatelimitHeadersAndWaitIfNeeded(headers, pds);
		return data;
	} catch (err) {
		if (err instanceof XRPCError && err.status === 429) {
			await parseRatelimitHeadersAndWaitIfNeeded(err.headers, pds);
		} else throw err;
	}
}

async function* listRepos(pds: string) {
	let cursor: string | undefined = "0";
	const rpc = new XRPC({ handler: new CredentialManager({ service: pds }) });
	while (cursor) {
		try {
			const { data: { repos, cursor: newCursor }, headers } = await rpc.get(
				"com.atproto.sync.listRepos",
				{ params: { limit: 1000, cursor } },
			);

			cursor = newCursor as string | undefined; // I do not know why this is necessary but the previous line errors otherwise

			for (const repo of repos) {
				yield repo.did;
			}

			await parseRatelimitHeadersAndWaitIfNeeded(headers, pds);
		} catch (err) {
			if (err instanceof XRPCError && err.status === 429) {
				await parseRatelimitHeadersAndWaitIfNeeded(err.headers, pds);
			} else throw err;
		}
	}
}

export async function getPdses(): Promise<Array<string>> {
	const atprotoScrapingData = await fetch(
		"https://raw.githubusercontent.com/mary-ext/atproto-scraping/refs/heads/trunk/state.json",
	).then((res) => res.ok ? res.json() : _throw("atproto-scraping state.json not ok")) as {
		pdses?: Record<string, unknown>;
	};

	if (!atprotoScrapingData.pdses) throw new Error("No pdses in atproto-scraping");

	const pdses = Object.keys(atprotoScrapingData.pdses).filter((pds) =>
		pds.startsWith("https://")
	);
	return pdses;
}

async function parseRatelimitHeadersAndWaitIfNeeded(headers: HeadersObject, pds: string) {
	const ratelimitRemaining = parseInt(headers["ratelimit-remaining"]);
	if (isNaN(ratelimitRemaining) || ratelimitRemaining < 1) {
		const ratelimitReset = parseInt(headers["ratelimit-reset"]) * 1000;
		if (isNaN(ratelimitReset)) {
			throw new Error("ratelimit-reset header is not a number for pds " + pds);
		} else {
			const now = Date.now();
			const waitTime = ratelimitReset - now + 3000; // add 3s to be safe
			if (waitTime > 0) {
				await sleep(waitTime);
			}
		}
	}
}

const _throw = (err: string) => {
	throw new Error(err);
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

void main();