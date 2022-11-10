use crate::utils::pkpv_to_string;
use anyhow::Result;

wit_bindgen_guest_rust::generate!({
    import: "../pancake_server/assets/db.wit",
    default: "../pancake_server/assets/udf.wit",
    name: "udf",
});
use db::{Pkpv, Sv, SvSpec};
use udf::CommitDecision;

// See `build.rs`.
include!(concat!(env!("OUT_DIR"), "/const_gen.rs"));

export_udf!(Udf);
pub struct Udf {}
impl udf::Udf for Udf {
    fn run_txn() -> Result<CommitDecision, String> {
        let sv_spec = SvSpec { bytes: THE_SV_SPEC };
        let sv_lo = Sv { bytes: THE_SV_LO };
        let sv_hi = Sv { bytes: THE_SV_HI };

        let pkpvs = db::get_sv_range(sv_spec, Some(sv_lo), Some(sv_hi))?;

        let mut ret = String::new();
        for Pkpv { pk, pv } in pkpvs {
            let s = pkpv_to_string(&pk.bytes, &pv.bytes)?;
            ret.push_str(&s);
        }

        Ok(CommitDecision::Commit(ret))
    }
}
