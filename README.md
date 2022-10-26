# 🥞

Pancake is an experimental database with the following features:

- Data model = Document store. Documents are dynamically typed.
- Secondary indexes work on key definitions that are one contiguous sub-portion of values.
- Storage engine = LSM Tree.
- Isolation = Serializable Snapshot Isolation (SSI) and Serial execution. There are two separate corresponding implementations of the storage engine.
- No partitioning or replication (yet).

## Architecture

See [doc diagrams](./doc).

Rustdoc is [here](https://ysono.github.io/pancake/pancake/index.html).

## Sample usage

```sh
ENGINE_VARIETY="${1:-ssi}"
DB="localhost:3000/${ENGINE_VARIETY}"

### Basic put/delete/get ###

curl -i -X PUT "${DB}/key/mykey" -d myvalue
curl -i -X GET "${DB}/key/mykey"
curl -i -X DELETE "${DB}/key/mykey"

### Queries ###

curl -i -X POST "${DB}/query" -d 'put int(100) str(1000)'
curl -i -X POST "${DB}/query" -d 'create index str'
curl -i -X POST "${DB}/query" -d 'get where str between str(10) str(12)'
```

For the full documentation on the query language, see [the rustdoc for query](https://ysono.github.io/pancake/pancake_server/query/basic/index.html).
