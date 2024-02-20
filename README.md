# 🥞

Pancake is a toy database. The project's goal is to learn and experiment with algorithms. As a product, Pancake works, possibly correctly, and mildly fastly.

Features:
- Data model = Document, dynamically typed.
- Each secondary index key definition covers one contiguous sub-portion of values.
    It is specified by a sequence of integers that inspect within nested tuples.
- Storage data structure = LSM Tree.
- Storage engines:
    - Serial execution.
    - MVCC implementing Serializable Snapshot Isolation (i.e. optimistic locking).
- Distributed:
    - No partitioning or replication, yet.

## Architecture

See [doc diagrams](./doc).

## Sample usage

Starting the server:

```sh
cargo run --package pancake_server --bin pancake_server_serial
cargo run --package pancake_server --bin pancake_server_ssi
```

Accessing the server:

- Simple CRUD by http method. See [this sample test script](./pancake_server/tests/pancake-test-script.sh) for examples.
- A [query language](https://ysono.github.io/pancake/pancake_server/oper/query_basic/index.html). See [this sample test script](./pancake_server/tests/pancake-test-script.sh) for examples.
- Transaction expressed as a [WASM component](https://github.com/WebAssembly/component-model). See [instruction](examples_wasm_txn/readme.md).
