use super::super::helpers::gen;
use super::helper_verify::verify_get;
use anyhow::Result;
use pancake::storage::db::DB;
use pancake::storage::serde::{Datum, DatumType};
use pancake::storage::types::{PrimaryKey, SubValue, SubValueSpec, Value};
use std::sync::Arc;

/// A spec that extracts `value[1][2]: str`.
fn spec_1_2_str() -> SubValueSpec {
    SubValueSpec {
        member_idxs: vec![1, 2],
        datum_type: DatumType::Str,
    }
}

/// A spec that extracts `value[1]: tuple`.
/// The type of the contents of the tuple is opaque in the view of this spec.
fn spec_1_tup() -> SubValueSpec {
    SubValueSpec {
        member_idxs: vec![1],
        datum_type: DatumType::Tuple,
    }
}

/// Value is a type that can be captured by both [`spec_1_2_str`] and [`spec_1_tup`] specs.
/// Specifically, its type is `(int, (int, int, str), int)`.
fn gen_pv(pv_i: i64, pv_s: &str) -> Value {
    let pv = Datum::Tuple(vec![
        Datum::I64(0),
        Datum::Tuple(vec![
            Datum::I64(pv_i),
            Datum::I64(0),
            Datum::Str(String::from(pv_s)),
        ]),
        Datum::I64(0),
    ]);
    Value(pv)
}

fn gen_pkv(pk: &str, pv_i: i64, pv_s: &str) -> (PrimaryKey, Value) {
    (gen::gen_str_pk(pk), gen_pv(pv_i, pv_s))
}

/// A string-typed SubValue.
/// This SubValue is workable with any spec that extracts a string-typed SubValue, such as [`spec_1_2_str`].
fn gen_sv_str(sv: &str) -> SubValue {
    gen::gen_str_sv(sv)
}

/// A SubValue typed `(int, int, str)`.
/// This is a type such that [`spec_1_tup`] can extract it from a value produced by [`kv`].
fn gen_sv_tup(sv_i: i64, sv_s: &str) -> SubValue {
    SubValue(Datum::Tuple(vec![
        Datum::I64(sv_i),
        Datum::I64(0),
        Datum::Str(String::from(sv_s)),
    ]))
}

fn put(db: &mut DB, pk: &str, pv_i: i64, pv_s: &str) -> Result<()> {
    let (pk, pv) = gen_pkv(pk, pv_i, pv_s);
    db.put(Arc::new(pk), Some(Arc::new(pv)))
}

fn del(db: &mut DB, pk: &str) -> Result<()> {
    let pk = gen::gen_str_pk(pk);
    db.put(Arc::new(pk), None)
}

pub fn delete_create_get(db: &mut DB) -> Result<()> {
    let spec_str = Arc::new(spec_1_2_str());
    let spec_tup = Arc::new(spec_1_tup());

    db.delete_scnd_idx(&spec_str)?;
    db.delete_scnd_idx(&spec_tup)?;

    verify_get(db, &spec_str, None, None, vec![])?;
    verify_get(db, &spec_tup, None, None, vec![])?;

    put(db, "complex.4", 40, "complex-subval")?;
    put(db, "complex.3", 30, "complex-subval")?;

    db.create_scnd_idx(Arc::clone(&spec_str))?;
    db.create_scnd_idx(Arc::clone(&spec_tup))?;

    put(db, "complex.2", 20, "complex-subval")?;
    put(db, "complex.1", 10, "complex-subval")?;

    verify_get(
        db,
        &spec_str,
        None,
        None,
        vec![
            gen_pkv("complex.1", 10, "complex-subval"),
            gen_pkv("complex.2", 20, "complex-subval"),
            gen_pkv("complex.3", 30, "complex-subval"),
            gen_pkv("complex.4", 40, "complex-subval"),
        ],
    )?;

    verify_get(
        db,
        &spec_str,
        Some(gen_sv_str("complex-subval")),
        Some(gen_sv_str("complex-subval")),
        vec![
            gen_pkv("complex.1", 10, "complex-subval"),
            gen_pkv("complex.2", 20, "complex-subval"),
            gen_pkv("complex.3", 30, "complex-subval"),
            gen_pkv("complex.4", 40, "complex-subval"),
        ],
    )?;

    verify_get(
        db,
        &spec_tup,
        Some(gen_sv_tup(20, "complex-")),
        None,
        vec![
            gen_pkv("complex.2", 20, "complex-subval"),
            gen_pkv("complex.3", 30, "complex-subval"),
            gen_pkv("complex.4", 40, "complex-subval"),
        ],
    )?;

    verify_get(
        db,
        &spec_tup,
        None,
        Some(gen_sv_tup(30, "complex-subval-zzzz")),
        vec![
            gen_pkv("complex.1", 10, "complex-subval"),
            gen_pkv("complex.2", 20, "complex-subval"),
            gen_pkv("complex.3", 30, "complex-subval"),
        ],
    )?;

    verify_get(
        db,
        &spec_tup,
        Some(gen_sv_tup(20, "complex-")),
        Some(gen_sv_tup(30, "complex-")),
        vec![gen_pkv("complex.2", 20, "complex-subval")],
    )?;

    del(db, "complex.3")?;

    verify_get(
        db,
        &spec_str,
        None,
        None,
        vec![
            gen_pkv("complex.1", 10, "complex-subval"),
            gen_pkv("complex.2", 20, "complex-subval"),
            gen_pkv("complex.4", 40, "complex-subval"),
        ],
    )?;

    Ok(())
}
