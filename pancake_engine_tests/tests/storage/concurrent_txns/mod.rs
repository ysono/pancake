mod dirty_w_r;
mod lost_update;
mod phantom;
mod repeatable_read;
mod write_skew;

use super::helpers::etc::coerce_ref_to_static;
use anyhow::Result;
use pancake_engine_ssi::DB;

pub async fn test_concurrent_txns(db: &DB) -> Result<()> {
    let db_ref = unsafe { coerce_ref_to_static(db) };

    // All below tests could be run concurrently.

    dirty_w_r::no_dirty_write(db_ref).await?;
    dirty_w_r::no_dirty_read(db_ref).await?;

    repeatable_read::repeatable_read(db_ref).await?;

    lost_update::no_lost_update(db_ref).await?;
    write_skew::no_write_skew(db_ref).await?;
    phantom::no_phantom(db_ref).await?;

    Ok(())
}
