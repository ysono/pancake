use anyhow::Result;
use pancake::storage::db::DB;
use pancake::storage::serde::{Datum, DatumType};
use pancake::storage::types::{PrimaryKey, SubValue, SubValueSpec, Value};

/// A spec that extracts `value[1][2]: str`.
fn spec_1_2_str() -> SubValueSpec {
    SubValueSpec::PartialTuple {
        member_idx: 1,
        member_spec: Box::new(SubValueSpec::PartialTuple {
            member_idx: 2,
            member_spec: Box::new(SubValueSpec::Whole(DatumType::Str)),
        }),
    }
}

/// A spec that extracts `value[1]: tuple`.
/// The type of the contents of the tuple is opaque in the view of this spec.
fn spec_1_tup() -> SubValueSpec {
    SubValueSpec::PartialTuple {
        member_idx: 1,
        member_spec: Box::new(SubValueSpec::Whole(DatumType::Tuple)),
    }
}

fn key(k: &str) -> PrimaryKey {
    let key = Datum::Str(String::from(k));
    PrimaryKey(key)
}

/// Value is a type that can be captured by both [`spec_1_2_str`] and [`spec_1_tup`] specs.
/// Specifically, its type is `(int, (int, int, str), int)`.
fn val(v_i: i64, v_s: &str) -> Value {
    let val = Datum::Tuple(vec![
        Datum::I64(0),
        Datum::Tuple(vec![
            Datum::I64(v_i),
            Datum::I64(0),
            Datum::Str(String::from(v_s)),
        ]),
        Datum::I64(0),
    ]);
    Value(val)
}

fn kv(k: &str, v_i: i64, v_s: &str) -> (PrimaryKey, Value) {
    (key(k), val(v_i, v_s))
}

/// A string-typed SubValue.
/// This SubValue is workable with any spec that extracts a string-typed SubValue, such as [`spec_1_2_str`].
fn subval_str(subval: &str) -> SubValue {
    SubValue(Datum::Str(String::from(subval)))
}

/// A SubValue typed `(int, int, str)`.
/// This is a type such that [`spec_1_tup`] can extract it from a value produced by [`kv`].
fn subval_tup(i: i64, s: &str) -> SubValue {
    SubValue(Datum::Tuple(vec![
        Datum::I64(i),
        Datum::I64(0),
        Datum::Str(String::from(s)),
    ]))
}

fn put(db: &mut DB, k: &str, v_i: i64, v_s: &str) -> Result<()> {
    let (k, v) = kv(k, v_i, v_s);
    db.put(k, v)
}

fn verify_get(
    db: &mut DB,
    spec: &SubValueSpec,
    subval_lo: Option<SubValue>,
    subval_hi: Option<SubValue>,
    exp: Vec<(PrimaryKey, Value)>,
) -> Result<bool> {
    let actual = db.get_by_sub_value(&spec, subval_lo.as_ref(), subval_hi.as_ref())?;

    let success = exp == actual;
    if !success {
        eprintln!("Expected {:?}; got {:?}", exp, actual);
    }
    Ok(success)
}

pub fn delete_create_get(db: &mut DB) -> Result<()> {
    let spec_str = spec_1_2_str();
    let spec_tup = spec_1_tup();

    db.delete_sec_idx(&spec_str)?;
    db.delete_sec_idx(&spec_tup)?;

    let mut success = true;

    let s = verify_get(db, &spec_str, None, None, vec![])?;
    success &= s;
    let s = verify_get(db, &spec_tup, None, None, vec![])?;
    success &= s;

    put(db, "complex.4", 40, "complex-subval")?;
    put(db, "complex.3", 30, "complex-subval")?;

    db.create_sec_idx(spec_str.clone())?;
    db.create_sec_idx(spec_tup.clone())?;

    put(db, "complex.2", 20, "complex-subval")?;
    put(db, "complex.1", 10, "complex-subval")?;

    let s = verify_get(
        db,
        &spec_str,
        None,
        None,
        vec![
            kv("complex.1", 10, "complex-subval"),
            kv("complex.2", 20, "complex-subval"),
            kv("complex.3", 30, "complex-subval"),
            kv("complex.4", 40, "complex-subval"),
        ],
    )?;
    success &= s;

    let s = verify_get(
        db,
        &spec_str,
        Some(subval_str("complex-subval")),
        Some(subval_str("complex-subval")),
        vec![
            kv("complex.1", 10, "complex-subval"),
            kv("complex.2", 20, "complex-subval"),
            kv("complex.3", 30, "complex-subval"),
            kv("complex.4", 40, "complex-subval"),
        ],
    )?;
    success &= s;

    let s = verify_get(
        db,
        &spec_tup,
        Some(subval_tup(20, "complex-")),
        None,
        vec![
            kv("complex.2", 20, "complex-subval"),
            kv("complex.3", 30, "complex-subval"),
            kv("complex.4", 40, "complex-subval"),
        ],
    )?;
    success &= s;

    let s = verify_get(
        db,
        &spec_tup,
        None,
        Some(subval_tup(30, "complex-subval-zzzz")),
        vec![
            kv("complex.1", 10, "complex-subval"),
            kv("complex.2", 20, "complex-subval"),
            kv("complex.3", 30, "complex-subval"),
        ],
    )?;
    success &= s;

    let s = verify_get(
        db,
        &spec_tup,
        Some(subval_tup(20, "complex-")),
        Some(subval_tup(30, "complex-")),
        vec![kv("complex.2", 20, "complex-subval")],
    )?;
    success &= s;

    db.delete(key("complex.3"))?;

    let s = verify_get(
        db,
        &spec_str,
        None,
        None,
        vec![
            kv("complex.1", 10, "complex-subval"),
            kv("complex.2", 20, "complex-subval"),
            kv("complex.4", 40, "complex-subval"),
        ],
    )?;
    success &= s;

    assert!(success);

    Ok(())
}
