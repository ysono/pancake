use super::super::helpers::gen;
use super::helper_verify::verify_get;
use anyhow::Result;
use pancake::storage::engine_serial::db::DB;
use pancake::storage::serde::DatumType;
use pancake::storage::types::SubValueSpec;
use std::sync::Arc;

fn put(db: &mut DB, pk: &str, pv: &str) -> Result<()> {
    let (pk, pv) = gen::gen_str_pkv(pk, pv);
    db.put(Arc::new(pk), Some(Arc::new(pv)))
}

fn del(db: &mut DB, pk: &str) -> Result<()> {
    let pk = gen::gen_str_pk(pk);
    db.put(Arc::new(pk), None)
}

pub fn delete_create_get(db: &mut DB) -> Result<()> {
    let spec = Arc::new(SubValueSpec::from(DatumType::Str));

    db.delete_scnd_idx(&spec)?;

    verify_get(db, &spec, None, None, Err(()))?;

    put(db, "g.1", "secidxtest-val-g")?;
    put(db, "f.1", "secidxtest-val-f")?;
    put(db, "e.1", "secidxtest-val-e")?;

    db.create_scnd_idx(Arc::clone(&spec))?;

    put(db, "g.2", "secidxtest-val-g")?;
    put(db, "f.2", "secidxtest-val-f")?;
    put(db, "e.2", "secidxtest-val-e")?;

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
    )?;

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
    )?;

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
    )?;

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
    )?;

    del(db, "f.1")?;

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
    )?;

    Ok(())
}
