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

# The tuple type nests other data.
curl -i -X POST "${DB}/query" -d 'put int(5000) tup( str("s5000") int(50) )'
curl -i -X POST "${DB}/query" -d 'get int(5000)'
curl -i -X POST "${DB}/query" -d 'put int(5001) tup( str("s5000") int(51) )'
curl -i -X POST "${DB}/query" -d 'get int(5001)'

# The tuple type can nest another tuple.
curl -i -X POST "${DB}/query" -d 'put int(6000) tup( str("s6000") tup( int(60) str("s60") ) int(60) )'
curl -i -X POST "${DB}/query" -d 'get int(6000)'
curl -i -X POST "${DB}/query" -d 'put int(6001) tup( str("s6000") tup( int(61) str("s61") ) int(61) )'
curl -i -X POST "${DB}/query" -d 'get int(6001)'

### Range query ###

curl -i -X POST "${DB}/query" -d 'get between int(50) int(150)'
curl -i -X POST "${DB}/query" -d 'get between int(50) _'
curl -i -X POST "${DB}/query" -d 'get between _ int(150)'
curl -i -X POST "${DB}/query" -d 'get between _ _'

### Querying by sub-portion of value ###

# Index all entries value type.
curl -i -X POST "${DB}/query" -d 'create index int'

# Index all entries by sub-value specification.
curl -i -X POST "${DB}/query" -d 'create index tup( 0 str )'

# Index all entries by nested sub-value specification.
curl -i -X POST "${DB}/query" -d 'create index tup( 1 tup( 0 int ) )'

# Get all entries by value type.
curl -i -X POST "${DB}/query" -d 'get where int _'
curl -i -X POST "${DB}/query" -d 'get where int int(1000)'
curl -i -X POST "${DB}/query" -d 'get where int between int(500) int(1500)'

# Get all entries by sub-value specification.
curl -i -X POST "${DB}/query" -d 'get where tup( 0 str ) _'
curl -i -X POST "${DB}/query" -d 'get where tup( 0 str ) str("s5000")'
curl -i -X POST "${DB}/query" -d 'get where tup( 0 str ) between str("s1000") str("s9000")'

# Get all entries by nested sub-value specification.
curl -i -X POST "${DB}/query" -d 'get where tup( 1 tup( 0 int ) ) _'
curl -i -X POST "${DB}/query" -d 'get where tup( 1 tup( 0 int ) ) int(60)'
curl -i -X POST "${DB}/query" -d 'get where tup( 1 tup( 0 int ) ) between int(60) int(61)'
```
