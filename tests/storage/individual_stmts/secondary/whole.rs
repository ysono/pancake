use super::super::super::helpers::{gen, one_stmt::OneStmtDbAdaptor};
use super::helper_verify::verify_get;
use anyhow::Result;
use pancake::storage::serde::DatumType;
use pancake::storage::types::SubValueSpec;
use std::sync::Arc;

async fn put(db: &mut impl OneStmtDbAdaptor, pk: &str, pv: &str) -> Result<()> {
    let (pk, pv) = gen::gen_str_pkv(pk, pv);
    db.put(Arc::new(pk), Some(Arc::new(pv))).await
}

async fn del(db: &mut impl OneStmtDbAdaptor, pk: &str) -> Result<()> {
    let pk = gen::gen_str_pk(pk);
    db.put(Arc::new(pk), None).await
}

pub async fn delete_create_get(db: &mut impl OneStmtDbAdaptor) -> Result<()> {
    let spec = Arc::new(SubValueSpec::from(DatumType::Str));

    db.delete_scnd_idx(&spec).await?;

    verify_get(db, &spec, None, None, Err(())).await?;

    put(db, "g.1", "secidxtest-val-g").await?;
    put(db, "f.1", "secidxtest-val-f").await?;
    put(db, "e.1", "secidxtest-val-e").await?;

    db.create_scnd_idx(Arc::clone(&spec)).await?;

    put(db, "g.2", "secidxtest-val-g").await?;
    put(db, "f.2", "secidxtest-val-f").await?;
    put(db, "e.2", "secidxtest-val-e").await?;

    verify_get(
        db,
        &spec,
        Some(gen::gen_str_sv("secidxtest-val-a")),
        Some(gen::gen_str_sv("secidxtest-val-z")),
        Ok(vec![
            gen::gen_str_pkv("e.1", "secidxtest-val-e"),
            gen::gen_str_pkv("e.2", "secidxtest-val-e"),
            gen::gen_str_pkv("f.1", "secidxtest-val-f"),
            gen::gen_str_pkv("f.2", "secidxtest-val-f"),
            gen::gen_str_pkv("g.1", "secidxtest-val-g"),
            gen::gen_str_pkv("g.2", "secidxtest-val-g"),
        ]),
    )
    .await?;

    verify_get(
        db,
        &spec,
        Some(gen::gen_str_sv("secidxtest-val-f")),
        Some(gen::gen_str_sv("secidxtest-val-z")),
        Ok(vec![
            gen::gen_str_pkv("f.1", "secidxtest-val-f"),
            gen::gen_str_pkv("f.2", "secidxtest-val-f"),
            gen::gen_str_pkv("g.1", "secidxtest-val-g"),
            gen::gen_str_pkv("g.2", "secidxtest-val-g"),
        ]),
    )
    .await?;

    verify_get(
        db,
        &spec,
        Some(gen::gen_str_sv("secidxtest-val-a")),
        Some(gen::gen_str_sv("secidxtest-val-f")),
        Ok(vec![
            gen::gen_str_pkv("e.1", "secidxtest-val-e"),
            gen::gen_str_pkv("e.2", "secidxtest-val-e"),
            gen::gen_str_pkv("f.1", "secidxtest-val-f"),
            gen::gen_str_pkv("f.2", "secidxtest-val-f"),
        ]),
    )
    .await?;

    verify_get(
        db,
        &spec,
        Some(gen::gen_str_sv("secidxtest-val-e")),
        Some(gen::gen_str_sv("secidxtest-val-g")),
        Ok(vec![
            gen::gen_str_pkv("e.1", "secidxtest-val-e"),
            gen::gen_str_pkv("e.2", "secidxtest-val-e"),
            gen::gen_str_pkv("f.1", "secidxtest-val-f"),
            gen::gen_str_pkv("f.2", "secidxtest-val-f"),
            gen::gen_str_pkv("g.1", "secidxtest-val-g"),
            gen::gen_str_pkv("g.2", "secidxtest-val-g"),
        ]),
    )
    .await?;

    del(db, "f.1").await?;

    verify_get(
        db,
        &spec,
        Some(gen::gen_str_sv("secidxtest-val-a")),
        Some(gen::gen_str_sv("secidxtest-val-z")),
        Ok(vec![
            gen::gen_str_pkv("e.1", "secidxtest-val-e"),
            gen::gen_str_pkv("e.2", "secidxtest-val-e"),
            gen::gen_str_pkv("f.2", "secidxtest-val-f"),
            gen::gen_str_pkv("g.1", "secidxtest-val-g"),
            gen::gen_str_pkv("g.2", "secidxtest-val-g"),
        ]),
    )
    .await?;

    Ok(())
}
