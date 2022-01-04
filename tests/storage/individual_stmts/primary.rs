use super::super::helpers::{gen, one_stmt::OneStmtDbAdaptor};
use anyhow::Result;
use pancake::storage::serde::Datum;
use pancake::storage::types::{PKShared, PVShared, PrimaryKey, Value};
use rand;
use std::collections::BTreeMap;
use std::sync::Arc;

pub async fn put_del_get_getrange(db: &mut impl OneStmtDbAdaptor) -> Result<()> {
    let mut pk_to_expected_pv = BTreeMap::<PKShared, Option<PVShared>>::new();

    let data_count = 100usize;

    // Insert random data. Then delete some of them randomly.
    {
        for _ in 0..data_count {
            let i = rand::random::<u16>();

            let pk = Arc::new(gen::gen_str_pk(format!("key{}", i)));
            let pv = Arc::new(gen::gen_str_pv(format!("val{}", i)));

            db.put(pk.clone(), Some(pv.clone())).await?;

            let keep = rand::random::<f32>() < 0.7;
            if keep {
                pk_to_expected_pv.insert(pk, Some(pv));
            } else {
                db.put(pk.clone(), None).await?;
                pk_to_expected_pv.insert(pk, None);
            }
        }

        for (pk, exp_pv) in pk_to_expected_pv.iter() {
            let act_pv = db.get_pk_one(pk).await?.map(|(_k, v)| v);
            assert_eq!(*exp_pv, act_pv);
        }
    }

    // Among the above-inserted data, query over a range in the middle.
    {
        let range_lo_i = data_count / 4;
        let range_hi_i = range_lo_i * 3;
        let exp_range = pk_to_expected_pv
            .into_iter()
            .skip(range_lo_i)
            .take(range_hi_i - range_lo_i)
            .filter_map(|(pk, opt_pv)| opt_pv.map(|pv| (pk, pv)))
            .collect::<Vec<_>>();
        assert!(exp_range.len() >= 3);

        let act_range = db
            .get_pk_range(Some(&exp_range[0].0), Some(&exp_range.last().unwrap().0))
            .await?;
        assert_eq!(exp_range, act_range);
    }

    Ok(())
}

pub async fn nonexistent(db: &mut impl OneStmtDbAdaptor) -> Result<()> {
    let pk = gen::gen_str_pk("nonexistent");

    let actual = db.get_pk_one(&pk).await?;
    assert!(actual.is_none());

    Ok(())
}

pub async fn zero_byte_value(db: &mut impl OneStmtDbAdaptor) -> Result<()> {
    let pk = Arc::new(gen::gen_str_pk("empty"));
    let pv = Arc::new(Value(Datum::Bytes(vec![])));

    db.put(pk.clone(), Some(pv.clone())).await?;

    let actual = db.get_pk_one(&pk).await?.map(|(_k, v)| v);
    assert_eq!(Some(pv), actual);

    Ok(())
}

pub async fn tuple(db: &mut impl OneStmtDbAdaptor) -> Result<()> {
    let pk = {
        let dat = Datum::Tuple(vec![
            Datum::Bytes(vec![16u8, 17u8, 18u8]),
            Datum::I64(0x123456789abcdef),
            Datum::Str(String::from("ahoy in tuple")),
        ]);
        Arc::new(PrimaryKey(dat))
    };

    let pv = {
        let dat = Datum::Tuple(vec![
            Datum::I64(0x1337),
            Datum::Bytes(vec![16u8, 17u8, 18u8]),
            Datum::Tuple(vec![
                Datum::Str(String::from("double-nested 1")),
                Datum::Str(String::from("double-nested 2")),
                Datum::Bytes(vec![0u8, 1u8]),
            ]),
            Datum::Tuple(vec![]),
        ]);
        Arc::new(Value(dat))
    };

    db.put(pk.clone(), Some(pv.clone())).await?;

    let actual = db.get_pk_one(&pk).await?.map(|(_k, v)| v);
    assert_eq!(Some(pv), actual);

    Ok(())
}
