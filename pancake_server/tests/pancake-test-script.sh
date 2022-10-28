#/usr/bin/env bash

set -e

ENGINE_VARIETY="${1:-ssi}"
DB="localhost:3000/${ENGINE_VARIETY}"

### Query by primary key, by http method ###

curl -f -i -X PUT "${DB}/key/mykey" -d myvalue
curl -f -i -X GET "${DB}/key/mykey"
curl -f -i -X DELETE "${DB}/key/mykey"
curl -f -i -X GET "${DB}/key/mykey"

### Query by primary key ###

curl -f -i -X POST "${DB}/query" -d 'put int(100) int(1000)'
curl -f -i -X POST "${DB}/query" -d 'get int(100)'
curl -f -i -X POST "${DB}/query" -d 'put int(101) int(1010)'
curl -f -i -X POST "${DB}/query" -d 'get int(101)'
curl -f -i -X POST "${DB}/query" -d 'put int(102) str(1020)'
curl -f -i -X POST "${DB}/query" -d 'del int(102)'
curl -f -i -X POST "${DB}/query" -d 'get int(102)'

curl -f -i -X POST "${DB}/query" -d 'put int(6000) tup( str(s6000) tup( int(60) str(s60) ) int(60) )'
curl -f -i -X POST "${DB}/query" -d 'get int(6000)'
curl -f -i -X POST "${DB}/query" -d 'put int(6001) tup( str(s6000) tup( int(61) str(s61) ) int(61) )'
curl -f -i -X POST "${DB}/query" -d 'get int(6001)'

### Query by primary key range ###

curl -f -i -X POST "${DB}/query" -d 'get between int(50) str(foobar)'
curl -f -i -X POST "${DB}/query" -d 'get between int(50) _'
curl -f -i -X POST "${DB}/query" -d 'get between _ str(foobar)'
curl -f -i -X POST "${DB}/query" -d 'get between _ _'

### Query by secondary key (i.e. sub-portion of value) ###

# Delete indexes
curl -i -X POST "${DB}/query" -d 'delete index svspec(int)'
curl -i -X POST "${DB}/query" -d 'delete index svspec(0 str)'
curl -i -X POST "${DB}/query" -d 'delete index svspec(1 0 int)'

# Create indexes
curl -f -i -X POST "${DB}/query" -d 'create index svspec(int)'
curl -f -i -X POST "${DB}/query" -d 'create index svspec(0 str)'
curl -f -i -X POST "${DB}/query" -d 'create index svspec(1 0 int)'

# Get all entries by whole-value.
curl -f -i -X POST "${DB}/query" -d 'get where svspec(int) int(1000)'
curl -f -i -X POST "${DB}/query" -d 'get where svspec(int) between int(500) int(1500)'
curl -f -i -X POST "${DB}/query" -d 'get where svspec(int) between _ int(1500)'
curl -f -i -X POST "${DB}/query" -d 'get where svspec(int) _'

# Get all entries by sub-value specification.
curl -f -i -X POST "${DB}/query" -d 'get where svspec(0 str) str(s6000)'
curl -f -i -X POST "${DB}/query" -d 'get where svspec(0 str) between str(s1000) str(s9000)'
curl -f -i -X POST "${DB}/query" -d 'get where svspec(0 str) _'

# Get all entries by nested sub-value specification.
curl -f -i -X POST "${DB}/query" -d 'get where svspec(1 0 int) int(60)'
curl -f -i -X POST "${DB}/query" -d 'get where svspec(1 0 int) between int(60) int(61)'
curl -f -i -X POST "${DB}/query" -d 'get where svspec(1 0 int) _'

# Delete indexes
curl -f -i -X POST "${DB}/query" -d 'delete index svspec(int)'
curl -f -i -X POST "${DB}/query" -d 'delete index svspec(0 str)'
curl -f -i -X POST "${DB}/query" -d 'delete index svspec(1 0 int)'
