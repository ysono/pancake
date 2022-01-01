use super::helpers::gen;
use anyhow::Result;
use pancake::storage::db::DB;
use pancake::storage::serde::Datum;
use pancake::storage::types::{PrimaryKey, Value};
use rand;
use std::collections::BTreeMap;

pub fn put_del_get_getrange(db: &mut DB) -> Result<()> {
    let mut pk_to_expected_pv = BTreeMap::<PrimaryKey, Option<Value>>::new();

    let data_count = 100usize;

    // Insert random data. Then delete some of them randomly.
    {
        for _ in 0..data_count {
            let i = rand::random::<u16>();

            let pk = gen::gen_str_pk(format!("key{}", i));
            let pv = gen::gen_str_pv(format!("val{}", i));

            db.put(pk.clone(), pv.clone())?;

            let keep = rand::random::<f32>() < 0.7;
            if keep {
                pk_to_expected_pv.insert(pk, Some(pv));
            } else {
                db.delete(pk.clone())?;
                pk_to_expected_pv.insert(pk, None);
            }
        }

        for (pk, exp_pv) in pk_to_expected_pv.iter() {
            let act_pv = db.get(pk).unwrap();
            assert_eq!(exp_pv, &act_pv);
        }
    }

    // Among the above-inserted data, query over a range in the middle.
    {
        let range_lo_i = data_count / 4;
        let range_hi_i = range_lo_i * 3;
        let exp_range = pk_to_expected_pv
            .iter()
            .skip(range_lo_i)
            .take(range_hi_i - range_lo_i)
            .filter_map(|(pk, opt_pv)| opt_pv.as_ref().map(|pv| (pk, pv)))
            .collect::<Vec<_>>();
        assert!(exp_range.len() >= 3);

        let act_range = db.get_range(Some(&exp_range[0].0), Some(&exp_range.last().unwrap().0))?;
        let act_range = act_range.iter().map(|(k, v)| (k, v)).collect::<Vec<_>>();
        assert_eq!(exp_range, act_range);
    }

    Ok(())
}

pub fn nonexistent(db: &mut DB) -> Result<()> {
    let pk = gen::gen_str_pk("nonexistent");

    let actual = db.get(&pk)?;
    assert!(actual.is_none());

    Ok(())
}

pub fn zero_byte_value(db: &mut DB) -> Result<()> {
    let pk = gen::gen_str_pk("empty");

    let pv = Value(Datum::Bytes(vec![]));

    db.put(pk.clone(), pv.clone())?;

    let actual = db.get(&pk)?;
    assert_eq!(Some(pv), actual);

    Ok(())
}

pub fn tuple(db: &mut DB) -> Result<()> {
    let pk = Datum::Tuple(vec![
        Datum::Bytes(vec![16u8, 17u8, 18u8]),
        Datum::I64(0x123456789abcdef),
        Datum::Str(String::from("ahoy in tuple")),
    ]);
    let pk = PrimaryKey(pk);

    let pv = Datum::Tuple(vec![
        Datum::I64(0x1337),
        Datum::Bytes(vec![16u8, 17u8, 18u8]),
        Datum::Tuple(vec![
            Datum::Str(String::from("double-nested 1")),
            Datum::Str(String::from("double-nested 2")),
            Datum::Bytes(vec![0u8, 1u8]),
        ]),
        Datum::Tuple(vec![]),
    ]);
    let pv = Value(pv);

    db.put(pk.clone(), pv.clone())?;

    let actual = db.get(&pk)?;
    assert_eq!(Some(pv), actual);

    Ok(())
}
