use anyhow::Result;
use pancake::storage::db::DB;
use pancake::storage::types::{PrimaryKey, SubValue, SubValueSpec, Value};
use std::sync::Arc;

pub fn verify_get(
    db: &mut DB,
    spec: &SubValueSpec,
    sv_lo: Option<SubValue>,
    sv_hi: Option<SubValue>,
    exp: Result<Vec<(PrimaryKey, Value)>, ()>,
) -> Result<()> {
    let act = db.get_sv_range(&spec, sv_lo.as_ref(), sv_hi.as_ref());
    match (exp, act) {
        (Err(_exp), Err(act)) => {
            assert_eq!(
                format!("Secondary index does not exist for {:?}", spec),
                act.to_string()
            )
        }
        (Ok(exp), Ok(act)) => {
            let exp = exp
                .into_iter()
                .map(|(pk, pv)| (Arc::new(pk), Arc::new(pv)))
                .collect::<Vec<_>>();
            let act = act
                .map(|entry| entry.take_kv())
                .collect::<Result<Vec<_>>>()?;
            assert_eq!(exp, act);
        }
        _etc => panic!("ok-err mistmatch"),
    }

    Ok(())
}
