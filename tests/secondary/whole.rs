use anyhow::Result;
use pancake::storage::db::DB;
use pancake::storage::serde::DatumType;
use pancake::storage::types::{Datum, PrimaryKey, SubValue, SubValueSpec, Value};

fn kv(k: &str, v: &str) -> (PrimaryKey, Value) {
    (
        PrimaryKey(Datum::Str(String::from(k))),
        Value(Datum::Str(String::from(v))),
    )
}

fn put(db: &mut DB, k: &str, v: &str) -> Result<()> {
    let (k, v) = kv(k, v);
    db.put(k, v)
}

fn verify_get(
    db: &mut DB,
    spec: &SubValueSpec,
    subval: &str,
    pk_lo: Option<&str>,
    pk_hi: Option<&str>,
    exp: Vec<(PrimaryKey, Value)>,
) -> Result<bool> {
    let subval = SubValue(Datum::Str(String::from(subval)));
    let pk_lo = pk_lo.map(|s| PrimaryKey(Datum::Str(String::from(s))));
    let pk_hi = pk_hi.map(|s| PrimaryKey(Datum::Str(String::from(s))));

    let actual = db.get_by_sub_value(&spec, &subval, pk_lo.as_ref(), pk_hi.as_ref())?;

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

    let s = verify_get(db, &spec, "val-a", None, None, vec![])?;
    success &= s;

    let s = verify_get(db, &spec, "val-b", None, None, vec![])?;
    success &= s;

    put(db, "a.1", "val-a")?;
    put(db, "a.2", "val-a")?;
    put(db, "b.1", "val-b")?;
    put(db, "b.2", "val-b")?;

    db.create_sec_idx(spec.clone())?;

    put(db, "a.3", "val-a")?;
    put(db, "b.3", "val-b")?;
    put(db, "a.4", "val-a")?;
    put(db, "b.4", "val-b")?;

    let s = verify_get(
        db,
        &spec,
        "val-a",
        None,
        None,
        vec![
            kv("a.1", "val-a"),
            kv("a.2", "val-a"),
            kv("a.3", "val-a"),
            kv("a.4", "val-a"),
        ],
    )?;
    success &= s;

    let s = verify_get(
        db,
        &spec,
        "val-b",
        None,
        None,
        vec![
            kv("b.1", "val-b"),
            kv("b.2", "val-b"),
            kv("b.3", "val-b"),
            kv("b.4", "val-b"),
        ],
    )?;
    success &= s;

    let s = verify_get(
        db,
        &spec,
        "val-a",
        Some("a.2"),
        Some("a.3"),
        vec![kv("a.2", "val-a"), kv("a.3", "val-a")],
    )?;
    success &= s;

    let s = verify_get(
        db,
        &spec,
        "val-a",
        Some("a.2"),
        None,
        vec![kv("a.2", "val-a"), kv("a.3", "val-a"), kv("a.4", "val-a")],
    )?;
    success &= s;

    let s = verify_get(
        db,
        &spec,
        "val-b",
        Some("b.0"),
        Some("b.9"),
        vec![
            kv("b.1", "val-b"),
            kv("b.2", "val-b"),
            kv("b.3", "val-b"),
            kv("b.4", "val-b"),
        ],
    )?;
    success &= s;

    assert!(success);

    Ok(())
}
