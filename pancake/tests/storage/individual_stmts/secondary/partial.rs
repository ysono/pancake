use super::super::super::helpers::gen;
use super::super::OneStmtDbAdaptor;
use super::helper_verify::verify_get;
use anyhow::Result;
use pancake::storage::serde::{Datum, DatumType};
use pancake::storage::types::{PrimaryKey, SubValue, SubValueSpec, Value};
use std::sync::Arc;

/// A SVSpec that extracts `PV[1][2]: str`.
fn spec_1_2_str() -> SubValueSpec {
    SubValueSpec {
        member_idxs: vec![1, 2],
        datum_type: DatumType::Str,
    }
}

/// A SVSpec that extracts `PV[1]: tuple`.
/// The contents of the targeted tuple are opaque in the view of this SVSpec.
fn spec_1_tup() -> SubValueSpec {
    SubValueSpec {
        member_idxs: vec![1],
        datum_type: DatumType::Tuple,
    }
}

/// A PV that is typed `(int, (int, int, str), int)`.
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

/// A SubValue that is typed `str`.
fn gen_sv_str(sv: &str) -> SubValue {
    gen::gen_str_sv(sv)
}

/// A SubValue that is typed `(int, int, str)`.
fn gen_sv_tup(sv_i: i64, sv_s: &str) -> SubValue {
    SubValue(Datum::Tuple(vec![
        Datum::I64(sv_i),
        Datum::I64(0),
        Datum::Str(String::from(sv_s)),
    ]))
}

async fn put(db: &mut impl OneStmtDbAdaptor, pk: &str, pv_i: i64, pv_s: &str) -> Result<()> {
    let (pk, pv) = gen_pkv(pk, pv_i, pv_s);
    db.put(Arc::new(pk), Some(Arc::new(pv))).await
}

async fn del(db: &mut impl OneStmtDbAdaptor, pk: &str) -> Result<()> {
    let pk = gen::gen_str_pk(pk);
    db.put(Arc::new(pk), None).await
}

pub async fn delete_create_get(db: &mut impl OneStmtDbAdaptor) -> Result<()> {
    let spec_1_2_str = Arc::new(spec_1_2_str());
    let spec_1_tup = Arc::new(spec_1_tup());

    /* Delete scnd idxs. */

    db.delete_scnd_idx(&spec_1_2_str).await?;
    db.delete_scnd_idx(&spec_1_tup).await?;

    verify_get(db, &spec_1_2_str, None, None, Err(())).await?;
    verify_get(db, &spec_1_tup, None, None, Err(())).await?;

    /* Insert ; Create scnd idxs ; Insert more. */

    put(db, "complex.8", 8, "complex-subval-8").await?;
    put(db, "complex.6", 6, "aaa-6").await?;
    put(db, "complex.4", 4, "complex-subval-4").await?;
    put(db, "complex.2", 2, "complex-subval-2").await?;

    db.create_scnd_idx(Arc::clone(&spec_1_2_str)).await?;
    db.create_scnd_idx(Arc::clone(&spec_1_tup)).await?;

    put(db, "complex.7", 7, "complex-subval-7").await?;
    put(db, "complex.5", 5, "aaa-5").await?;
    put(db, "complex.3", 3, "complex-subval-3").await?;
    put(db, "complex.1", 1, "complex-subval-1").await?;

    /* Get by range of SVs @ PV[1]: tup. */

    verify_get(
        db,
        &spec_1_tup,
        None,
        None,
        Ok(vec![
            gen_pkv("complex.1", 1, "complex-subval-1"),
            gen_pkv("complex.2", 2, "complex-subval-2"),
            gen_pkv("complex.3", 3, "complex-subval-3"),
            gen_pkv("complex.4", 4, "complex-subval-4"),
            gen_pkv("complex.5", 5, "aaa-5"),
            gen_pkv("complex.6", 6, "aaa-6"),
            gen_pkv("complex.7", 7, "complex-subval-7"),
            gen_pkv("complex.8", 8, "complex-subval-8"),
        ]),
    )
    .await?;

    verify_get(
        db,
        &spec_1_tup,
        Some(gen_sv_tup(2, "complex-")),
        None,
        Ok(vec![
            gen_pkv("complex.2", 2, "complex-subval-2"),
            gen_pkv("complex.3", 3, "complex-subval-3"),
            gen_pkv("complex.4", 4, "complex-subval-4"),
            gen_pkv("complex.5", 5, "aaa-5"),
            gen_pkv("complex.6", 6, "aaa-6"),
            gen_pkv("complex.7", 7, "complex-subval-7"),
            gen_pkv("complex.8", 8, "complex-subval-8"),
        ]),
    )
    .await?;

    verify_get(
        db,
        &spec_1_tup,
        None,
        Some(gen_sv_tup(7, "complex-subval-999")),
        Ok(vec![
            gen_pkv("complex.1", 1, "complex-subval-1"),
            gen_pkv("complex.2", 2, "complex-subval-2"),
            gen_pkv("complex.3", 3, "complex-subval-3"),
            gen_pkv("complex.4", 4, "complex-subval-4"),
            gen_pkv("complex.5", 5, "aaa-5"),
            gen_pkv("complex.6", 6, "aaa-6"),
            gen_pkv("complex.7", 7, "complex-subval-7"),
        ]),
    )
    .await?;

    verify_get(
        db,
        &spec_1_tup,
        Some(gen_sv_tup(2, "complex-")),
        Some(gen_sv_tup(8, "complex-")),
        Ok(vec![
            gen_pkv("complex.2", 2, "complex-subval-2"),
            gen_pkv("complex.3", 3, "complex-subval-3"),
            gen_pkv("complex.4", 4, "complex-subval-4"),
            gen_pkv("complex.5", 5, "aaa-5"),
            gen_pkv("complex.6", 6, "aaa-6"),
            gen_pkv("complex.7", 7, "complex-subval-7"),
        ]),
    )
    .await?;

    /* Get by range of SVs @ PV[1][2]: str. */

    verify_get(
        db,
        &spec_1_2_str,
        None,
        None,
        Ok(vec![
            gen_pkv("complex.5", 5, "aaa-5"),
            gen_pkv("complex.6", 6, "aaa-6"),
            gen_pkv("complex.1", 1, "complex-subval-1"),
            gen_pkv("complex.2", 2, "complex-subval-2"),
            gen_pkv("complex.3", 3, "complex-subval-3"),
            gen_pkv("complex.4", 4, "complex-subval-4"),
            gen_pkv("complex.7", 7, "complex-subval-7"),
            gen_pkv("complex.8", 8, "complex-subval-8"),
        ]),
    )
    .await?;

    verify_get(
        db,
        &spec_1_2_str,
        Some(gen_sv_str("complex-subval")),
        Some(gen_sv_str("complex-subval-999")),
        Ok(vec![
            gen_pkv("complex.1", 1, "complex-subval-1"),
            gen_pkv("complex.2", 2, "complex-subval-2"),
            gen_pkv("complex.3", 3, "complex-subval-3"),
            gen_pkv("complex.4", 4, "complex-subval-4"),
            gen_pkv("complex.7", 7, "complex-subval-7"),
            gen_pkv("complex.8", 8, "complex-subval-8"),
        ]),
    )
    .await?;

    /* Modify ; Get. */

    // Bring PV[1][2] out of midrange.
    put(db, "complex.4", 444, "aaa-444").await?;
    // Bring PV[1][2] into midrange.
    put(db, "complex.6", 666, "complex-subval-666").await?;
    // Keep PV[1][2] out of midrange.
    put(db, "complex.5", 555, "aaa-555").await?;
    // Keep PV[1][2] inside midrange.
    put(db, "complex.3", 333, "complex-subval-333").await?;

    verify_get(
        db,
        &spec_1_tup,
        None,
        None,
        Ok(vec![
            gen_pkv("complex.1", 1, "complex-subval-1"),
            gen_pkv("complex.2", 2, "complex-subval-2"),
            gen_pkv("complex.7", 7, "complex-subval-7"),
            gen_pkv("complex.8", 8, "complex-subval-8"),
            gen_pkv("complex.3", 333, "complex-subval-333"),
            gen_pkv("complex.4", 444, "aaa-444"),
            gen_pkv("complex.5", 555, "aaa-555"),
            gen_pkv("complex.6", 666, "complex-subval-666"),
        ]),
    )
    .await?;

    verify_get(
        db,
        &spec_1_tup,
        Some(gen_sv_tup(5, "")),
        Some(gen_sv_tup(500, "")),
        Ok(vec![
            gen_pkv("complex.7", 7, "complex-subval-7"),
            gen_pkv("complex.8", 8, "complex-subval-8"),
            gen_pkv("complex.3", 333, "complex-subval-333"),
            gen_pkv("complex.4", 444, "aaa-444"),
        ]),
    )
    .await?;

    verify_get(
        db,
        &spec_1_2_str,
        None,
        None,
        Ok(vec![
            gen_pkv("complex.4", 444, "aaa-444"),
            gen_pkv("complex.5", 555, "aaa-555"),
            gen_pkv("complex.1", 1, "complex-subval-1"),
            gen_pkv("complex.2", 2, "complex-subval-2"),
            gen_pkv("complex.3", 333, "complex-subval-333"),
            gen_pkv("complex.6", 666, "complex-subval-666"),
            gen_pkv("complex.7", 7, "complex-subval-7"),
            gen_pkv("complex.8", 8, "complex-subval-8"),
        ]),
    )
    .await?;

    verify_get(
        db,
        &spec_1_2_str,
        Some(gen_sv_str("complex-subval")),
        Some(gen_sv_str("complex-subval-777")),
        Ok(vec![
            gen_pkv("complex.1", 1, "complex-subval-1"),
            gen_pkv("complex.2", 2, "complex-subval-2"),
            gen_pkv("complex.3", 333, "complex-subval-333"),
            gen_pkv("complex.6", 666, "complex-subval-666"),
            gen_pkv("complex.7", 7, "complex-subval-7"),
        ]),
    )
    .await?;

    /* Delete ; Get. */

    del(db, "complex.3").await?;

    verify_get(
        db,
        &spec_1_tup,
        None,
        None,
        Ok(vec![
            gen_pkv("complex.1", 1, "complex-subval-1"),
            gen_pkv("complex.2", 2, "complex-subval-2"),
            gen_pkv("complex.7", 7, "complex-subval-7"),
            gen_pkv("complex.8", 8, "complex-subval-8"),
            gen_pkv("complex.4", 444, "aaa-444"),
            gen_pkv("complex.5", 555, "aaa-555"),
            gen_pkv("complex.6", 666, "complex-subval-666"),
        ]),
    )
    .await?;

    verify_get(
        db,
        &spec_1_2_str,
        None,
        None,
        Ok(vec![
            gen_pkv("complex.4", 444, "aaa-444"),
            gen_pkv("complex.5", 555, "aaa-555"),
            gen_pkv("complex.1", 1, "complex-subval-1"),
            gen_pkv("complex.2", 2, "complex-subval-2"),
            gen_pkv("complex.6", 666, "complex-subval-666"),
            gen_pkv("complex.7", 7, "complex-subval-7"),
            gen_pkv("complex.8", 8, "complex-subval-8"),
        ]),
    )
    .await?;

    Ok(())
}
