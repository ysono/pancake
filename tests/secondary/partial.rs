use anyhow::Result;
use pancake::storage::db::DB;
use pancake::storage::serde::DatumType;
use pancake::storage::types::{Datum, PrimaryKey, SubValue, SubValueSpec, Value};

fn kv(k: &str, v: &str) -> (PrimaryKey, Value) {
    let key = Datum::Str(String::from(k));
    let key = PrimaryKey(key);

    let val = Datum::Tuple(vec![
        Datum::I64(0),
        Datum::Tuple(vec![
            Datum::I64(1),
            Datum::I64(1),
            Datum::Str(String::from(v)),
        ]),
        Datum::I64(0),
    ]);
    let val = Value(val);

    (key, val)
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
    let spec = SubValueSpec::PartialTuple {
        member_idx: 1,
        member_spec: Box::new(SubValueSpec::PartialTuple {
            member_idx: 2,
            member_spec: Box::new(SubValueSpec::Whole(DatumType::Str)),
        }),
    };

    db.delete_sec_idx(&spec)?;

    let mut success = true;

    let s = verify_get(db, &spec, "complex-subval", None, None, vec![])?;
    success &= s;

    put(db, "complex.1", "complex-subval")?;
    put(db, "complex.2", "complex-subval")?;

    db.create_sec_idx(spec.clone())?;

    put(db, "complex.3", "complex-subval")?;
    put(db, "complex.4", "complex-subval")?;

    let s = verify_get(
        db,
        &spec,
        "complex-subval",
        None,
        None,
        vec![
            kv("complex.1", "complex-subval"),
            kv("complex.2", "complex-subval"),
            kv("complex.3", "complex-subval"),
            kv("complex.4", "complex-subval"),
        ],
    )?;
    success &= s;

    let s = verify_get(
        db,
        &spec,
        "complex-subval",
        Some("complex.2"),
        Some("complex.3"),
        vec![
            kv("complex.2", "complex-subval"),
            kv("complex.3", "complex-subval"),
        ],
    )?;
    success &= s;

    assert!(success);

    Ok(())
}
