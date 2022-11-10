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

Starting the server:

```sh
cargo run --package pancake_server
```

Accessing the server:

- Simple CRUD by http method.
- A [query language](https://ysono.github.io/pancake/pancake_server/query/basic/index.html). See [this sample test script](./pancake_server/tests/pancake-test-script.sh) for examples.
- Transaction expressed as a [WASM component](https://github.com/WebAssembly/component-model). See [instruction](examples_wasm_txn/readme.md).
