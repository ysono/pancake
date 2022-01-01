use super::super::helpers::gen;
use super::helper_verify::verify_get;
use anyhow::Result;
use pancake::storage::db::DB;
use pancake::storage::serde::DatumType;
use pancake::storage::types::SubValueSpec;

fn put(db: &mut DB, pk: &str, pv: &str) -> Result<()> {
    let (pk, pv) = gen::gen_str_pkv(pk, pv);
    db.put(pk, pv)
}

pub fn delete_create_get(db: &mut DB) -> Result<()> {
    let spec = SubValueSpec::Whole(DatumType::Str);

    db.delete_sec_idx(&spec)?;

    verify_get(db, &spec, None, None, vec![])?;

    put(db, "g.1", "secidxtest-val-g")?;
    put(db, "f.1", "secidxtest-val-f")?;
    put(db, "e.1", "secidxtest-val-e")?;

    db.create_sec_idx(spec.clone())?;

    put(db, "g.2", "secidxtest-val-g")?;
    put(db, "f.2", "secidxtest-val-f")?;
    put(db, "e.2", "secidxtest-val-e")?;

    verify_get(
        db,
        &spec,
        Some(gen::gen_str_sv("secidxtest-val-a")),
        Some(gen::gen_str_sv("secidxtest-val-z")),
        vec![
            gen::gen_str_pkv("e.1", "secidxtest-val-e"),
            gen::gen_str_pkv("e.2", "secidxtest-val-e"),
            gen::gen_str_pkv("f.1", "secidxtest-val-f"),
            gen::gen_str_pkv("f.2", "secidxtest-val-f"),
            gen::gen_str_pkv("g.1", "secidxtest-val-g"),
            gen::gen_str_pkv("g.2", "secidxtest-val-g"),
        ],
    )?;

    verify_get(
        db,
        &spec,
        Some(gen::gen_str_sv("secidxtest-val-f")),
        Some(gen::gen_str_sv("secidxtest-val-z")),
        vec![
            gen::gen_str_pkv("f.1", "secidxtest-val-f"),
            gen::gen_str_pkv("f.2", "secidxtest-val-f"),
            gen::gen_str_pkv("g.1", "secidxtest-val-g"),
            gen::gen_str_pkv("g.2", "secidxtest-val-g"),
        ],
    )?;

    verify_get(
        db,
        &spec,
        Some(gen::gen_str_sv("secidxtest-val-a")),
        Some(gen::gen_str_sv("secidxtest-val-f")),
        vec![
            gen::gen_str_pkv("e.1", "secidxtest-val-e"),
            gen::gen_str_pkv("e.2", "secidxtest-val-e"),
            gen::gen_str_pkv("f.1", "secidxtest-val-f"),
            gen::gen_str_pkv("f.2", "secidxtest-val-f"),
        ],
    )?;

    verify_get(
        db,
        &spec,
        Some(gen::gen_str_sv("secidxtest-val-e")),
        Some(gen::gen_str_sv("secidxtest-val-g")),
        vec![
            gen::gen_str_pkv("e.1", "secidxtest-val-e"),
            gen::gen_str_pkv("e.2", "secidxtest-val-e"),
            gen::gen_str_pkv("f.1", "secidxtest-val-f"),
            gen::gen_str_pkv("f.2", "secidxtest-val-f"),
            gen::gen_str_pkv("g.1", "secidxtest-val-g"),
            gen::gen_str_pkv("g.2", "secidxtest-val-g"),
        ],
    )?;

    db.delete(gen::gen_str_pk("f.1"))?;

    verify_get(
        db,
        &spec,
        Some(gen::gen_str_sv("secidxtest-val-a")),
        Some(gen::gen_str_sv("secidxtest-val-z")),
        vec![
            gen::gen_str_pkv("e.1", "secidxtest-val-e"),
            gen::gen_str_pkv("e.2", "secidxtest-val-e"),
            gen::gen_str_pkv("f.2", "secidxtest-val-f"),
            gen::gen_str_pkv("g.1", "secidxtest-val-g"),
            gen::gen_str_pkv("g.2", "secidxtest-val-g"),
        ],
    )?;

    Ok(())
}
