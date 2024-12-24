import { pack, unpack } from "msgpackr";
import fs from "node:fs"

async function main(argv: string[]) {
    if (argv.length != 1) {
        console.log(`USAGE: ${process.argv[1]} DID-CACHE-PATH`)
        process.exit(1)
    }

    const cachePath = argv[0]
    const repos = unpack(fs.readFileSync("dids.cache"))
    for (const repo of repos) {
        console.log(JSON.stringify(repo))
    }
}
main(process.argv.slice(2))