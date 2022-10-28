# 🥞

Pancake is an experimental database with the following features:

- Data model = Document store. Documents are dynamically typed.
- Secondary indexes are defined on one contiguous sub-portion of values.
- Storage data structure = LSM Tree.
- Isolation = Serializable Snapshot Isolation (SSI; i.e. optimistic locking) and Serial execution. There are two separate corresponding implementations of the storage engine.
- No partitioning or replication (yet).

## Architecture

See [doc diagrams](./doc).

## Sample usage

Start the server:

```sh
cargo run --package pancake_server
```

Access the server:

See [this sample test script](./pancake_server/tests/pancake-test-script.sh) for an example.

For the full documentation on the query language, see [the rustdoc for query](https://ysono.github.io/pancake/pancake_server/query/basic/index.html).
