# 🥞

> A database is but a 3D-printed pancake emoji -- Anonymous

### Usage

```sh
DB="localhost:3000"

### Basic put/delete/get ###

# Values and keys are both interpreted as string data.
curl -i -X PUT "${DB}/key/mykey" -d myvalue
curl -i -X GET "${DB}/key/mykey"
curl -i -X DELETE "${DB}/key/mykey"
curl -i -X GET "${DB}/key/mykey"

### Querying by key ###

# Keys and values are typed.
curl -i -X POST "${DB}/query" -d 'put int(100) int(1000)'
curl -i -X POST "${DB}/query" -d 'get int(100)'
curl -i -X POST "${DB}/query" -d 'put int(101) int(1010)'
curl -i -X POST "${DB}/query" -d 'get int(101)'
curl -i -X POST "${DB}/query" -d 'put int(102) str("1020")'
curl -i -X POST "${DB}/query" -d 'del int(102)'
curl -i -X POST "${DB}/query" -d 'get int(102)'

# The tuple type nests other data, including other tuples.
curl -i -X POST "${DB}/query" -d 'put int(6000) tup( str("s6000") tup( int(60) str("s60") ) int(60) )'
curl -i -X POST "${DB}/query" -d 'get int(6000)'
curl -i -X POST "${DB}/query" -d 'put int(6001) tup( str("s6000") tup( int(61) str("s61") ) int(61) )'
curl -i -X POST "${DB}/query" -d 'get int(6001)'

### Range query ###

# Analogous to:
# - `SELECT * FROM table WHERE pk BETWEEN ${pk_lo} AND ${pk_hi};`
# - `SELECT * FROM table WHERE pk <= ${pk_hi};`
# - etc
# Note, the comparison between keys is untyped, so the range might cover some data you don't expect.
curl -i -X POST "${DB}/query" -d 'get between int(50) str("foobar")'
curl -i -X POST "${DB}/query" -d 'get between int(50) _'
curl -i -X POST "${DB}/query" -d 'get between _ str("foobar")'
curl -i -X POST "${DB}/query" -d 'get between _ _'

### Querying by sub-portion of value ###

# Index creation queries are analogous to:
# `CREATE INDEX ON table (${column});`
# The difference is that, whereas a RDBMS allows specifing an index based on
# a selection of one or more columns, we support a selection of any of:
# - The whole value
# - One sub-portion of value at a specific nested location and having a specific type

# Index all entries by value type.
curl -i -X POST "${DB}/query" -d 'create index int'

# Index all entries by sub-value specification.
curl -i -X POST "${DB}/query" -d 'create index tup( 0 str )'

# Index all entries by nested sub-value specification.
curl -i -X POST "${DB}/query" -d 'create index tup( 1 tup( 0 int ) )'

# Index-dependent selection queries are analogous to:
# - `SELECT * FROM table WHERE ${column} = ${col_val};`
# - `SELECT * FROM table WHERE ${column} BETWEEN ${col_val_lo} AND ${col_val_hi};`
# - `SELECT * FROM table WHERE ${column} <= ${col_val_hi};`
# - etc
#
# In addition, b/c value schemas are dynamic, we also support selecting all values
# that match a spec, regardless of the sub-portion of value pointed to by the spec.
# It would be analogous to this hypothetical sql:
# `SELECT * FROM table WHERE ${column} IS VALID COLUMN;`

# Get all entries by value type.
curl -i -X POST "${DB}/query" -d 'get where int int(1000)'
curl -i -X POST "${DB}/query" -d 'get where int between int(500) int(1500)'
curl -i -X POST "${DB}/query" -d 'get where int between _ int(1500)'
curl -i -X POST "${DB}/query" -d 'get where int _'

# Get all entries by sub-value specification.
curl -i -X POST "${DB}/query" -d 'get where tup( 0 str ) str("s6000")'
curl -i -X POST "${DB}/query" -d 'get where tup( 0 str ) between str("s1000") str("s9000")'
curl -i -X POST "${DB}/query" -d 'get where tup( 0 str ) _'

# Get all entries by nested sub-value specification.
curl -i -X POST "${DB}/query" -d 'get where tup( 1 tup( 0 int ) ) int(60)'
curl -i -X POST "${DB}/query" -d 'get where tup( 1 tup( 0 int ) ) between int(60) int(61)'
curl -i -X POST "${DB}/query" -d 'get where tup( 1 tup( 0 int ) ) _'
```
