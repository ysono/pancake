use anyhow::Result;
use pancake::storage::db::DB;
use pancake::storage::types::{Datum, PrimaryKey, Value};
use rand;
use std::collections::BTreeMap;
use std::env::temp_dir;

#[test]
fn test_in_single_thread() -> Result<()> {
    let dir = temp_dir().join("pancake");
    let mut db = DB::open(dir)?;

    put_then_tomb(&mut db)?;
    nonexistent(&mut db)?;
    zero_byte_value(&mut db)?;
    tuple(&mut db)?;
    put_then_tomb(&mut db)?;
    Ok(())
}

fn put_then_tomb(db: &mut DB) -> Result<()> {
    let mut k_to_expected_v = BTreeMap::<PrimaryKey, Option<Value>>::new();

    for _ in 0..100 {
        let i = rand::random::<u16>();

        let key = PrimaryKey(Datum::Str(format!("key{}", i)));
        let val = Value(Datum::Str(format!("val{}", i)));

        db.put(key.clone(), val.clone())?;

        let keep = rand::random::<f32>() < 0.7;
        if keep {
            k_to_expected_v.insert(key, Some(val));
        } else {
            db.delete(key.clone())?;
            k_to_expected_v.insert(key, None);
        }
    }

    for (k, exp_v) in k_to_expected_v {
        let actual_v = db.get(k).unwrap();
        if exp_v != actual_v {
            panic!("Expected {:?}; got {:?}", exp_v, actual_v);
        }
    }

    Ok(())
}

fn nonexistent(db: &mut DB) -> Result<()> {
    let key = PrimaryKey(Datum::Str(String::from("nonexistent")));

    let res = db.get(key)?;

    assert!(res.is_none());

    Ok(())
}

fn zero_byte_value(db: &mut DB) -> Result<()> {
    let key = PrimaryKey(Datum::Str(String::from("empty")));

    let val = Value(Datum::Bytes(vec![]));

    db.put(key.clone(), val.clone())?;

    let res = db.get(key)?;

    if !(res.is_some() && res.as_ref().unwrap() == &val) {
        panic!("Expected {:?}; got {:?}", val, res);
    }

    Ok(())
}

fn tuple(db: &mut DB) -> Result<()> {
    let key = Datum::Tuple(vec![
        Datum::Bytes(vec![16u8, 17u8, 18u8]),
        Datum::I64(0x123456789abcdef),
        Datum::Str(String::from("ahoy in tuple")),
    ]);
    let key = PrimaryKey(key);

    let val = Datum::Tuple(vec![
        Datum::I64(0x1337),
        Datum::Tuple(vec![
            Datum::Str(String::from("double-nested 1")),
            Datum::Str(String::from("double-nested 2")),
            Datum::Str(String::from("double-nested 3")),
        ]),
        Datum::Tuple(vec![]),
        Datum::I64(0x7331),
    ]);
    let val = Value(val);

    db.put(key.clone(), val.clone())?;

    let res = db.get(key)?;

    if !(res.is_some() && res.as_ref().unwrap() == &val) {
        panic!("Expected {:?}; got {:?}", val, res);
    }

    Ok(())
}
