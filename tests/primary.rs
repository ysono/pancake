use anyhow::Result;
use pancake::storage::db::DB;
use pancake::storage::types::{Datum, PrimaryKey, Value};
use rand;
use std::collections::BTreeMap;

pub fn put_del_get_getrange(db: &mut DB) -> Result<()> {
    let mut k_to_expected_v = BTreeMap::<PrimaryKey, Option<Value>>::new();

    let data_count = 100usize;

    for _ in 0..data_count {
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

    for (k, exp_v) in k_to_expected_v.iter() {
        let actual_v = db.get(k).unwrap();
        if exp_v != &actual_v {
            panic!("Expected {:?}; got {:?}", exp_v, actual_v);
        }
    }

    let range_lo_i = data_count / 4;
    let range_hi_i = range_lo_i * 3;
    let exp_range = k_to_expected_v
        .iter()
        .skip(range_lo_i)
        .take(range_hi_i - range_lo_i)
        .filter_map(|(k, opt_v)| opt_v.as_ref().map(|v| (k, v)))
        .collect::<Vec<_>>();
    assert!(exp_range.len() >= 3);

    let act_range = db.get_range(Some(&exp_range[0].0), Some(&exp_range.last().unwrap().0))?;
    let act_range = act_range.iter().map(|(k, v)| (k, v)).collect::<Vec<_>>();

    if exp_range != act_range {
        panic!("Expected {:?}; got {:?}", exp_range, act_range);
    }

    Ok(())
}

pub fn nonexistent(db: &mut DB) -> Result<()> {
    let key = PrimaryKey(Datum::Str(String::from("nonexistent")));

    let res = db.get(&key)?;

    assert!(res.is_none());

    Ok(())
}

pub fn zero_byte_value(db: &mut DB) -> Result<()> {
    let key = PrimaryKey(Datum::Str(String::from("empty")));

    let val = Value(Datum::Bytes(vec![]));

    db.put(key.clone(), val.clone())?;

    let res = db.get(&key)?;

    if !(res.is_some() && res.as_ref().unwrap() == &val) {
        panic!("Expected {:?}; got {:?}", val, res);
    }

    Ok(())
}

pub fn tuple(db: &mut DB) -> Result<()> {
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

    let res = db.get(&key)?;

    if !(res.is_some() && res.as_ref().unwrap() == &val) {
        panic!("Expected {:?}; got {:?}", val, res);
    }

    Ok(())
}
