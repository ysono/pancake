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
        let pk_lo = PkParam { bytes: THE_PK_LO };
        let pk_hi = PkParam { bytes: THE_PK_HI };

        let pkpvs = db::get_pk_range(Some(pk_lo), Some(pk_hi))?;

        let mut ret = String::new();
        for Pkpv { pk, pv } in pkpvs {
            let s = pkpv_to_string(&pk.bytes, &pv.bytes)?;
            ret.push_str(&s);
        }

        Ok(CommitDecision::Commit(ret))
    }
}
