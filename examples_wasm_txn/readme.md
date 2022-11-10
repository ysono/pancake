## Building

```sh
PANCAKE_DIR="/path/to/pancake/"
cd "${PANCAKE_DIR}"
cargo build --package examples_wasm_txn --target wasm32-unknown-unknown --release --features "${FEATURE}"

WIT_BINDGEN_DIR="/path/to/wit-bindgen/"
cd "${WIT_BINDGEN_DIR}"
git checkout <the rev or tag as specified in Cargo.toml>
cargo run --release --package wit-component --bin wit-component -- --output component.wasm "${PANCAKE_DIR}/target/wasm32-unknown-unknown/release/examples_wasm_txn.wasm"

# These above two compilation steps will soon be combined by https://github.com/bytecodealliance/cargo-component
```

## Running

```sh
cargo run --package pancake_server

ENGINE_VARIETY="${1:-ssi}"
DB="localhost:3000/${ENGINE_VARIETY}"

# Prepare some data which the wasm examples will query.
curl -i -X POST "${DB}/query" -d 'put str(wasm_key_0) str(wasm_val_0)'
curl -i -X POST "${DB}/query" -d 'put str(wasm_key_0a) str(wasm_val_0a)'
curl -i -X POST "${DB}/query" -d 'put str(foo500) int(500)'
curl -i -X POST "${DB}/query" -d 'put str(foo501) int(501)'

# Do query.
curl -i -X POST "${DB}/wasm" --data-binary "@${WIT_BINDGEN_DIR}/component.wasm"
curl -i -X POST "${DB}/wasm" --data-binary "@${WIT_BINDGEN_DIR}/component.wasm"
```

## Known bugs

`simple_get_sv_range` on serial -- host crashes when writing `Vec<Pkpv>`.
`simple_get_sv_range` on ssi -- sometimes data are missing. Is this an ssi engine bug?

---

## TODO architecture

- Understand which conditions cause `wit-bindgen` to do O(n) copying between host and guest, assuming guest is built by rust. Eg reading from MemLog produces references; could these also avoid copying?
- Consider, in `pancake_engine_*` and `pancake_server` packages, making the primary data representation flat-bytes, rather than rust-native. This distinction is obvious for `Datum::Tuple`; but it also applies to other simple `Datum` types, as the rust-native representation represents the discriminant differently.
    - Currently:
        - Database types:
            - disk: flat bytes
            - pancake_engine_*: rust repr (`PrimaryKey`, `Value`, `SubValue`, ...)
            - wit-bindgen: host <-> bytes <-> guest
            - guest: lang repr
            - pancake_server: query string <-> rust repr
        - Guest's output:
            - guest: lang repr of string
            - wit-bindgen: guest <-> bytes <-> host
            - pancake_server: rust repr `String`
    - Improvement:
        - Database types:
            - disk: flat bytes
            - pancake_engine_*: flat bytes
            - wit-bindgen: host <-> bytes <-> guest ;; and when read from SSTable as owned data, it's possible to not do O(n) copy.
            - guest: lang repr ;; if using rust, can benefit from `pancake_types` lib's flat-bytes-based repr.
            - pancake_server: query string <-> flat bytes
        - Guest's output:
            - Ditto as database types. Everything is repr'd as bytes.
