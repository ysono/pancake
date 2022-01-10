# 🥞

Pancake is an experimental database with the following features:

- Data model = Document store. Documents are dynamically typed.
- Secondary indexes work on key definitions that are one contiguous sub-portion of values.
- Storage engine = LSM Tree.
- Operations are executed serially.

## Architecture

See [doc diagrams](./doc).

Rustdoc is [here](https://ysono.github.io/pancake/pancake/index.html).

## Usage

```sh
DB="localhost:3000"

### Basic put/delete/get ###

curl -i -X PUT "${DB}/key/mykey" -d myvalue
curl -i -X DELETE "${DB}/key/mykey"
curl -i -X GET "${DB}/key/mykey"

### Queries ###

curl -i -X POST "${DB}/query" -d 'put int(100) str("1000")'
curl -i -X POST "${DB}/query" -d 'get where int int(1000)'
curl -i -X POST "${DB}/query" -d 'get where tup( 1 tup( 0 int ) ) between int(60) int(61)'
```

For the full documentation on the query language, see [the rustdoc for query](https://ysono.github.io/pancake/pancake/frontend/query/basic/index.html).
