use anyhow::Result;

wit_bindgen_guest_rust::generate!({
    import: "../pancake_server/assets/db.wit",
    default: "../pancake_server/assets/udf.wit",
    name: "udf",
});
use db::{PkParam, PvParam};
use udf::CommitDecision;

// See `build.rs`.
include!(concat!(env!("OUT_DIR"), "/const_gen.rs"));

export_udf!(Udf);
pub struct Udf {}
impl udf::Udf for Udf {
    fn run_txn() -> Result<CommitDecision, String> {
        let pk = PkParam { bytes: THE_PK };
        let pv = PvParam { bytes: THE_PV };

        db::put(pk, Some(pv))?;

        Ok(CommitDecision::Commit(String::from("")))
    }
}
