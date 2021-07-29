use anyhow::Result;
use pancake::storage::api::*;
use pancake::storage::lsm;
use rand;
use std::collections::BTreeMap;

fn put_then_tomb() -> Result<()> {
    let mut lsm = lsm::LSM::init()?;

    let mut k_to_expected_v = BTreeMap::<Key, Value>::new();

    for _ in 0..500 {
        let i = rand::random::<u16>();

        let key = Key(Datum::Str(format!("key{}", i)));
        let mut val = Value::from(Datum::Str(format!("val{}", i)));

        lsm.put(key.clone(), val.clone())?;

        let keep = rand::random::<f32>() < 0.7;
        if !keep {
            val = Value(None);
            lsm.put(key.clone(), val.clone())?;
        }

        k_to_expected_v.insert(key, val);
    }

    for (k, exp_v) in k_to_expected_v {
        let actual_v = lsm.get(k).unwrap();
        if exp_v != actual_v {
            panic!("Expected {:?}; got {:?}", exp_v, actual_v);
        }
    }

    Ok(())
}

fn nonexistent() -> Result<()> {
    let lsm = lsm::LSM::init()?;

    let key = Key(Datum::Str(String::from("nonexistent")));

    let res = lsm.get(key)?;

    assert!(res.is_none());

    Ok(())
}

fn zero_byte_value() -> Result<()> {
    let mut lsm = lsm::LSM::init()?;

    let key = Key(Datum::Str(String::from("empty")));

    let val = Value::from(Datum::Bytes(vec![]));

    lsm.put(key.clone(), val.clone())?;

    let res = lsm.get(key)?;

    if val != res {
        panic!("Expected {:?}; got {:?}", val, res);
    }

    Ok(())
}

fn tuple() -> Result<()> {
    let mut lsm = lsm::LSM::init()?;

    let key = Key(Datum::Tuple(vec![
        Datum::Bytes(vec![16u8, 17u8, 18u8]),
        Datum::I64(0x123456789abcdef),
        Datum::Str(String::from("ahoy in tuple")),
    ]));

    let val = Value::from(Datum::Tuple(vec![
        Datum::I64(0x1337),
        Datum::Tuple(vec![
            Datum::Str(String::from("double-nested 1")),
            Datum::Str(String::from("double-nested 2")),
            Datum::Str(String::from("double-nested 3")),
        ]),
        Datum::Tuple(vec![]),
        Datum::I64(0x7331),
    ]));

    lsm.put(key.clone(), val.clone())?;

    let res = lsm.get(key)?;
    println!("{:?}", res);

    if res != val {
        panic!("Mismatch {:?} {:?}", res, val);
    }

    Ok(())
}

#[test]
fn test_in_single_thread() -> Result<()> {
    put_then_tomb()?;
    nonexistent()?;
    zero_byte_value()?;
    tuple()?;
    put_then_tomb()?;
    Ok(())
}
