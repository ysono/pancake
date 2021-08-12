use anyhow::Result;
use pancake::storage::db::DB;
use pancake::storage::serde::DatumType;
use pancake::storage::types::{Datum, PrimaryKey, SubValue, SubValueSpec, Value};

fn key(k: &str) -> PrimaryKey {
    PrimaryKey(Datum::Str(String::from(k)))
}

fn val(v: &str) -> Value {
    Value(Datum::Str(String::from(v)))
}

fn kv(k: &str, v: &str) -> (PrimaryKey, Value) {
    (key(k), val(v))
}

fn put(db: &mut DB, k: &str, v: &str) -> Result<()> {
    let (k, v) = kv(k, v);
    db.put(k, v)
}

fn verify_get(
    db: &mut DB,
    spec: &SubValueSpec,
    subval_lo: Option<&str>,
    subval_hi: Option<&str>,
    exp: Vec<(PrimaryKey, Value)>,
) -> Result<bool> {
    let subval_lo = subval_lo.map(|s| SubValue(Datum::Str(String::from(s))));
    let subval_hi = subval_hi.map(|s| SubValue(Datum::Str(String::from(s))));

    let actual = db.get_by_sub_value(&spec, subval_lo.as_ref(), subval_hi.as_ref())?;

    let success = exp == actual;
    if !success {
        eprintln!("Expected {:?}; got {:?}", exp, actual);
    }
    Ok(success)
}

pub fn delete_create_get(db: &mut DB) -> Result<()> {
    let spec = SubValueSpec::Whole(DatumType::Str);

    db.delete_sec_idx(&spec)?;

    let mut success = true;

    let s = verify_get(db, &spec, None, None, vec![])?;
    success &= s;

    put(db, "g.1", "secidxtest-val-g")?;
    put(db, "f.1", "secidxtest-val-f")?;
    put(db, "e.1", "secidxtest-val-e")?;

    db.create_sec_idx(spec.clone())?;

    put(db, "g.2", "secidxtest-val-g")?;
    put(db, "f.2", "secidxtest-val-f")?;
    put(db, "e.2", "secidxtest-val-e")?;

    let s = verify_get(
        db,
        &spec,
        Some("secidxtest-val-a"),
        Some("secidxtest-val-z"),
        vec![
            kv("e.1", "secidxtest-val-e"),
            kv("e.2", "secidxtest-val-e"),
            kv("f.1", "secidxtest-val-f"),
            kv("f.2", "secidxtest-val-f"),
            kv("g.1", "secidxtest-val-g"),
            kv("g.2", "secidxtest-val-g"),
        ],
    )?;
    success &= s;

    let s = verify_get(
        db,
        &spec,
        Some("secidxtest-val-f"),
        Some("secidxtest-val-z"),
        vec![
            kv("f.1", "secidxtest-val-f"),
            kv("f.2", "secidxtest-val-f"),
            kv("g.1", "secidxtest-val-g"),
            kv("g.2", "secidxtest-val-g"),
        ],
    )?;
    success &= s;

    let s = verify_get(
        db,
        &spec,
        Some("secidxtest-val-a"),
        Some("secidxtest-val-f"),
        vec![
            kv("e.1", "secidxtest-val-e"),
            kv("e.2", "secidxtest-val-e"),
            kv("f.1", "secidxtest-val-f"),
            kv("f.2", "secidxtest-val-f"),
        ],
    )?;
    success &= s;

    let s = verify_get(
        db,
        &spec,
        Some("secidxtest-val-e"),
        Some("secidxtest-val-g"),
        vec![
            kv("e.1", "secidxtest-val-e"),
            kv("e.2", "secidxtest-val-e"),
            kv("f.1", "secidxtest-val-f"),
            kv("f.2", "secidxtest-val-f"),
            kv("g.1", "secidxtest-val-g"),
            kv("g.2", "secidxtest-val-g"),
        ],
    )?;
    success &= s;

    db.delete(key("f.1"))?;

    let s = verify_get(
        db,
        &spec,
        Some("secidxtest-val-a"),
        Some("secidxtest-val-z"),
        vec![
            kv("e.1", "secidxtest-val-e"),
            kv("e.2", "secidxtest-val-e"),
            kv("f.2", "secidxtest-val-f"),
            kv("g.1", "secidxtest-val-g"),
            kv("g.2", "secidxtest-val-g"),
        ],
    )?;
    success &= s;

    assert!(success);

    Ok(())
}
