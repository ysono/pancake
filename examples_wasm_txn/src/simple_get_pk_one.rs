use crate::utils::pkpv_to_string;
use anyhow::Result;

wit_bindgen_guest_rust::generate!({
    import: "../pancake_server/assets/db.wit",
    default: "../pancake_server/assets/udf.wit",
    name: "udf",
});
use db::{PkParam, Pkpv};
use udf::CommitDecision;

// See `build.rs`.
include!(concat!(env!("OUT_DIR"), "/const_gen.rs"));

export_udf!(Udf);
pub struct Udf {}
impl udf::Udf for Udf {
    fn run_txn() -> Result<CommitDecision, String> {
        let pk = PkParam { bytes: THE_PK };

        let opt_pkpv = db::get_pk_one(pk)?;

        let ret = match opt_pkpv {
            None => format!("Not found"),
            Some(Pkpv { pk, pv }) => pkpv_to_string(&pk.bytes, &pv.bytes)?,
        };

        Ok(CommitDecision::Commit(ret))
    }
}
