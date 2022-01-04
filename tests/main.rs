use anyhow::Result;
use pancake::storage::engine_serial::db::DB;
use std::env;
use std::fs;

mod storage;
use storage::{primary, secondary};

#[test]
fn test_main() -> Result<()> {
    let db_dir = env::temp_dir().join("pancake");
    if db_dir.exists() {
        /* Don't remove the dir itself, so that symbolic links remain valid.
        This is for tester's convenience only.*/
        for sub in fs::read_dir(&db_dir)? {
            let sub = sub?.path();
            let meta = fs::metadata(&sub)?;
            if meta.is_file() {
                fs::remove_file(sub)?;
            } else {
                fs::remove_dir_all(sub)?;
            }
        }
    }
    let mut db = DB::load_or_new(db_dir)?;

    primary::put_del_get_getrange(&mut db)?;
    primary::nonexistent(&mut db)?;
    primary::zero_byte_value(&mut db)?;
    primary::tuple(&mut db)?;

    secondary::whole::delete_create_get(&mut db)?;
    secondary::partial::delete_create_get(&mut db)?;

    Ok(())
}
