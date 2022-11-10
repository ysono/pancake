use anyhow::Result;
use const_gen::{const_declaration, CompileConst};
use pancake_types::{
    serde::{Datum, DatumType},
    types::{PrimaryKey, Ser, SubValue, SubValueSpec, Value},
};
use std::env;
use std::fs;
use std::path::Path;

/// Build static parameters to statements, as bytes, so that there is no serialization at runtime.
fn main() -> Result<()> {
    let const_declarations = if cfg!(feature = "simple_get_pk_one") || cfg!(feature = "simple_del")
    {
        pk()?
    } else if cfg!(feature = "simple_get_pk_range") {
        pk_lo_hi()?
    } else if cfg!(feature = "simple_get_sv_range") {
        sv()?
    } else if cfg!(feature = "simple_put") {
        pk_pv()?
    } else {
        String::from("")
    };

    let out_dir = env::var_os("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("const_gen.rs");
    fs::write(&dest_path, const_declarations).unwrap();

    println!("cargo:rerun-if-changed=build.rs");

    Ok(())
}

fn pk() -> Result<String> {
    let pk_bytes = PrimaryKey(Datum::Str(String::from("wasm_key_0"))).ser_solo()?;

    let const_declarations = [const_declaration!(THE_PK = pk_bytes)].join("\n");

    Ok(const_declarations)
}
fn pk_lo_hi() -> Result<String> {
    let pk_lo_bytes = PrimaryKey(Datum::Str(String::from("wasm_key_0"))).ser_solo()?;
    let pk_hi_bytes = PrimaryKey(Datum::Str(String::from("wasm_key_1"))).ser_solo()?;

    let const_declarations = [
        const_declaration!(THE_PK_LO = pk_lo_bytes),
        const_declaration!(THE_PK_HI = pk_hi_bytes),
    ]
    .join("\n");

    Ok(const_declarations)
}
fn sv() -> Result<String> {
    let sv_spec = SubValueSpec {
        member_idxs: vec![],
        datum_type: DatumType::I64,
    }
    .ser_solo()?;
    let sv_lo_bytes = SubValue(Datum::I64(0)).ser_solo()?;
    let sv_hi_bytes = SubValue(Datum::I64(999)).ser_solo()?;

    let const_declarations = [
        const_declaration!(THE_SV_SPEC = sv_spec),
        const_declaration!(THE_SV_LO = sv_lo_bytes),
        const_declaration!(THE_SV_HI = sv_hi_bytes),
    ]
    .join("\n");

    Ok(const_declarations)
}
fn pk_pv() -> Result<String> {
    let pk_bytes = PrimaryKey(Datum::Str(String::from("wasm_key_0"))).ser_solo()?;

    let pv_bytes = Value(Datum::Str(String::from("wasm_val_0_updated_by_wasm"))).ser_solo()?;

    let const_declarations = [
        const_declaration!(THE_PK = pk_bytes),
        const_declaration!(THE_PV = pv_bytes),
    ]
    .join("\n");

    Ok(const_declarations)
}
