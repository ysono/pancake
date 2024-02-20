#!/usr/bin/env bash

set -eE

SERVER_PID=''
cleanup() {
    if [[ "${SERVER_PID}" != '' ]] ; then
        kill "${SERVER_PID}" || true
    fi
}
trap 'echo Failed!; cleanup' ERR

req() {
    ### Request; print response; assert status code.

    local exp_resp_code="$1"
    local exp_resp_line="HTTP/1.1 ${exp_resp_code}"
    
    resp="$(
        set -x
        curl -i --no-progress-meter -X "${@:2}"
    )"
    echo "${resp}"
    echo "${resp}" | grep "${exp_resp_line}"
}

crud() {
    local db="$1"

    ### Query by primary key, by http method ###

    req 204 PUT    "${db}/key/mykey" -d myvalue
    req 200 GET    "${db}/key/mykey"
    req 204 DELETE "${db}/key/mykey"
    req 404 GET    "${db}/key/mykey"

    ### Query by primary key ###

    req 204 POST "${db}/query" -d 'put int(100) int(1000)'
    req 200 POST "${db}/query" -d 'get int(100)'
    req 204 POST "${db}/query" -d 'put int(101) int(1010)'
    req 200 POST "${db}/query" -d 'get int(101)'
    req 204 POST "${db}/query" -d 'put int(102) str(1020)'
    req 204 POST "${db}/query" -d 'del int(102)'
    req 404 POST "${db}/query" -d 'get int(102)'

    req 204 POST "${db}/query" -d 'put int(6000) tup( str(s6000) tup( int(60) str(s60) ) int(60) )'
    req 200 POST "${db}/query" -d 'get int(6000)'
    req 204 POST "${db}/query" -d 'put int(6001) tup( str(s6000) tup( int(61) str(s61) ) int(61) )'
    req 200 POST "${db}/query" -d 'get int(6001)'

    ### Query by primary key range ###

    req 200 POST "${db}/query" -d 'get between int(6000) str(mykeyz)'
    req 200 POST "${db}/query" -d 'get between int(6000) _'
    req 200 POST "${db}/query" -d 'get between _ str(mykeyz)'
    req 200 POST "${db}/query" -d 'get between _ _'

    ### Query by secondary key (i.e. sub-portion of value) ###

    # Delete indexes
    req 204 POST "${db}/query" -d 'delete index svspec(int)'
    req 204 POST "${db}/query" -d 'delete index svspec(0 str)'
    req 204 POST "${db}/query" -d 'delete index svspec(1 0 int)'

    # # Create indexes
    req 204 POST "${db}/query" -d 'create index svspec(int)'
    req 204 POST "${db}/query" -d 'create index svspec(0 str)'
    req 204 POST "${db}/query" -d 'create index svspec(1 0 int)'

    # Get all entries by whole-value.
    req 200 POST "${db}/query" -d 'get where svspec(int) int(1000)'
    req 200 POST "${db}/query" -d 'get where svspec(int) between int(500) int(1500)'
    req 200 POST "${db}/query" -d 'get where svspec(int) between _ int(1500)'
    req 200 POST "${db}/query" -d 'get where svspec(int) _'

    # Get all entries by sub-value specification.
    req 200 POST "${db}/query" -d 'get where svspec(0 str) str(s6000)'
    req 200 POST "${db}/query" -d 'get where svspec(0 str) between str(s1000) str(s9000)'
    req 200 POST "${db}/query" -d 'get where svspec(0 str) _'

    # Get all entries by nested sub-value specification.
    req 200 POST "${db}/query" -d 'get where svspec(1 0 int) int(60)'
    req 200 POST "${db}/query" -d 'get where svspec(1 0 int) between int(60) int(61)'
    req 200 POST "${db}/query" -d 'get where svspec(1 0 int) _'

    # Delete indexes
    req 204 POST "${db}/query" -d 'delete index svspec(int)'
    req 204 POST "${db}/query" -d 'delete index svspec(0 str)'
    # req 204 POST "${db}/query" -d 'delete index svspec(1 0 int)'   # Don't delete, b/c we want to use it later.
}

assert_existing_data() {
    local db="$1"

    req 200 POST "${db}/query" -d 'get int(6000)'

    req 200 POST "${db}/query" -d 'get where svspec(1 0 int) int(60)'
}

count_bound_addrs() {
    local bind_addr="$1"

    echo "$(lsof -i -n -P | grep "${bind_addr}" | wc -l)"
}

launch_server() {
    local root_dir="$1"
    local bind_addr="$2"
    local bin_name="$3"

    local ct="$(count_bound_addrs "${bind_addr}")"
    if (( ct != 0 )) ; then
        echo "Cannot bind address ${bind_addr}"
        false
    fi

    PANCAKE_ROOT_DIR="${root_dir}" \
    PANCAKE_BIND_ADDR="${bind_addr}" \
        cargo run --package pancake_server --bin "${bin_name}" &
    SERVER_PID="$!"

    while : ; do
        local ct="$(count_bound_addrs "${bind_addr}")"
        if (( ct >= 1 )) ; then
            break
        fi
    done
}

test_server() {
    local root_dir="$1"
    local bind_addr="$2"
    local bin_name="$3"

    ### Launch. Then, send crud requests.

    launch_server "${root_dir}" "${bind_addr}" "${bin_name}"

    crud "${bind_addr}"

    kill "${SERVER_PID}"

    ### Launch again. Then, check existing data.

    launch_server "${root_dir}" "${bind_addr}" "${bin_name}"

    assert_existing_data "${bind_addr}"

    kill "${SERVER_PID}"
}

parent_dir="/tmp/pancake-$(date +'%s.%N')"
echo "parent_dir is ${parent_dir}"
rm -r "${parent_dir}" 2>/dev/null || true

test_server "${parent_dir}/serial" '127.0.0.1:3000' 'pancake_server_serial'
test_server "${parent_dir}/ssi"    '127.0.0.1:3001' 'pancake_server_ssi'

echo 'Success'

cleanup
