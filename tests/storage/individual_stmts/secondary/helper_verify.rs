use super::super::super::helpers::one_stmt::OneStmtDbAdaptor;
use anyhow::Result;
use pancake::storage::types::{PrimaryKey, SubValue, SubValueSpec, Value};
use std::sync::Arc;

pub async fn verify_get(
    db: &mut impl OneStmtDbAdaptor,
    spec: &SubValueSpec,
    sv_lo: Option<SubValue>,
    sv_hi: Option<SubValue>,
    exp: Result<Vec<(PrimaryKey, Value)>, ()>,
) -> Result<()> {
    let act = db.get_sv_range(&spec, sv_lo.as_ref(), sv_hi.as_ref()).await;
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
            assert_eq!(exp, act);
        }
        _etc => panic!("ok-err mistmatch"),
    }

    Ok(())
}
