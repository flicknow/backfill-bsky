import { fetchPdses } from "./util/fetch.js";

declare global {
	namespace NodeJS {
		interface ProcessEnv {
			BSKY_REPO_PROVIDER: string;
			BGS_ADMIN_KEY: string;
		}
	}
}

for (const envVar of ["BSKY_REPO_PROVIDER", "BGS_ADMIN_KEY"]) {
	if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

async function main() {
	const pdses = (await fetchPdses()).map((url) => new URL(url));

	const bgs = "https://" + process.env.BSKY_REPO_PROVIDER.replace(/^[a-z]+:\/\//, "");

	if (!process.argv.includes("--no-crawl")) {
		console.log("Requesting crawls...");
		await Promise.all(pdses.map(async (url) => {
			try {
				const res = await fetch(`${ bgs }/admin/pds/requestCrawl`, {
					method: "POST",
					headers: {
						"Content-Type": "application/json",
						Authorization: `Bearer ${ process.env.BGS_ADMIN_KEY }`,
					},
					body: JSON.stringify({ hostname: "https://" + url.hostname }),
				});
				if (!res.ok) {
					console.error(
						`Error requesting crawl for ${ url.hostname }: ${ res.status } ${ res.statusText } — ${ await res
							.json().then((r: any) => r?.error || "unknown error") }`,
					);
				}
			} catch (err) {
				console.error(`Network error requesting crawl for ${ url.hostname }: ${ err }`);
			}
		}));
		console.log("Done crawling!");
	}
	
	if (!process.argv.includes("--no-change-limits")) {
		console.log("Setting rate limits...");
		for (const url of pdses) {
		try {
			const limitsRes = await fetch(`${bgs}/admin/pds/changeLimits`, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					Authorization: `Bearer ${process.env.BGS_ADMIN_KEY}`,
				},
				body: JSON.stringify({
					host: url.hostname,
					perSecond: 10_000,
					perHour: 10_000_000,
					perDay: 200_000_000,
					repoLimit: 1_000_000,
					crawlRate: 100_000,
				}),
			});
			if (!limitsRes.ok) {
				console.error(
					`Error setting rate limits for ${url.hostname}: ${limitsRes.status} ${limitsRes.statusText} — ${await limitsRes
						.json().then((r: any) => r?.error || "unknown error")}`,
				);
			}
		} catch (err) {
			console.error(`Network error setting rate limits for ${url.hostname}: ${err}`);
		}
	}
		console.log("Done setting rate limits!");
	}
}

void main();
