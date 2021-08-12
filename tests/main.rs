use anyhow::Result;
use pancake::storage::db::DB;
use pancake::storage::utils;
use std::env;
use std::fs;

mod primary;
mod secondary;

#[test]
fn test_main() -> Result<()> {
    let dir = env::temp_dir().join("pancake");
    if dir.exists() {
        for subdir in utils::read_dir(&dir)? {
            fs::remove_dir_all(subdir)?;
        }
    }
    let mut db = DB::open(dir)?;

    primary::put_del_get_getrange(&mut db)?;
    primary::nonexistent(&mut db)?;
    primary::zero_byte_value(&mut db)?;
    primary::tuple(&mut db)?;

    secondary::whole::delete_create_get(&mut db)?;
    secondary::partial::delete_create_get(&mut db)?;

    Ok(())
}
