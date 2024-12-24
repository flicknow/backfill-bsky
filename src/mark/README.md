# DESCRIPTION

set of sripts for running the backfill without clustering to simplify profiling

producer.ts fetches repo records and writes to unix sockets that consumer.ts listen on

consumer.ts also includes benchmark tests for testing different types of inserts with fake follow record data

# setup

## env vars

### required

- BSKY_DB_POSTGRES_URL
- BSKY_DB_POSTGRES_SCHEMA
- BSKY_REPO_PROVIDER
- BSKY_DID_PLC_URL

### optional

- CONSUMER_BATCH_SIZE
- CONSUMER_TRANSACTION_SIZE
- CONSUMER_PG_NATIVE
- LOGGING_INTERVAL
- PRODUCER_CONCURRENCY
- PRODUCER_PROCESSING_TIMEOUT

## db

you can use the migrate.ts script to initialize your db

## did file

a json lines file of did's and pds's, used as the source for producer.ts

can generate from a did.cache file using unpack-as-did-file.ts

# scripts

## migrate.ts

run any db migrations not applied

### OPTIONS

#### --recreate-schema

drop and recreate the schema

#### --drop-follow-indexes

drop all indexes and constraints on the follow table. mainly useful if benchmarking consumer.ts with --db-test

## unpack-as-did-file.ts DID-CACHE-PATH

a packed did.cache file and prints jsonl of the entries to stdout
for best performance pipe through sort when storing (sorts on the dids which helps randomize the pds's a bit)

### ARGUMENTS

#### DID-CACHE-PATH

path to the did.cache file

## producer.ts DID-FILE SOCKET-PATH...

fetch repo records from dids in DID-FILE, and writes them to the SOCKET-PATHs

### ARGUMENTS

#### DID-FILE

jsonl file of dids and pds. can be generated from unpack-as-did-file.ts

#### SOCKET-PATH

path to unix socket that a consumer.ts is listening on. can take one or three paths. if three paths, the socket written to will be chosen based on the collection type of the record.

### OPTIONS

#### --starting-line NUMBER

line number in the DID-FILE to start reading did's from. mainly intended if you want to partition a DID-FILE and run multiple producer.ts processes

#### --ending-line NUMBER

line number in the DID-FILE to stop reading did's from. mainly intended if you want to partition a DID-FILE and run multiple producer.ts processes

## consumer.ts SOCKET-PATH

listens on a unix socket at SOCKET-PATH and indexes them in postgres

### ARGUMENTS

#### SOCKET-PATH

path to the unix socket to listen on

### OPTIONS

#### --db-test TEST

do not listen for events and insert randomly generated follow records. can be one of

- no-params
  - generate insert sql with no bind params. allows greater batch size than with-params
- with-params
  - generate insert sql with bind params
- with-prepared
  - cache prepared insert statements
