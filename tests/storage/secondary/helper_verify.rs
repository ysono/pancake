use anyhow::Result;
use pancake::storage::db::DB;
use pancake::storage::types::{PrimaryKey, SubValue, SubValueSpec, Value};
use std::sync::Arc;

pub fn verify_get(
    db: &mut DB,
    spec: &SubValueSpec,
    sv_lo: Option<SubValue>,
    sv_hi: Option<SubValue>,
    exp: Vec<(PrimaryKey, Value)>,
) -> Result<()> {
    let actual = db.get_sv_range(&spec, sv_lo.as_ref(), sv_hi.as_ref())?;

    let exp = exp
        .into_iter()
        .map(|(pk, pv)| (Arc::new(pk), Arc::new(pv)))
        .collect::<Vec<_>>();

    assert_eq!(exp, actual);

    Ok(())
}
