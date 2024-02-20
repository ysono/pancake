use std::env;
use std::path::PathBuf;

/// The parent of serial and ssi dirs.
pub const ENV_VAR_PARENT_DIR: &str = "PANCAKE_PARENT_DIR";

pub const SERIAL_DB_ROOT_NAME: &str = "serial";
pub const SSI_DB_ROOT_NAME: &str = "ssi";

#[derive(Clone, Copy)]
pub enum EngineType {
    SERIAL,
    SSI,
}

pub fn default_db_root_dir(typ: EngineType) -> PathBuf {
    let parent_dir_path = env::var(ENV_VAR_PARENT_DIR)
        .map_or_else(|_| env::temp_dir().join("pancake"), PathBuf::from);
    let db_dir_name = match typ {
        EngineType::SERIAL => SERIAL_DB_ROOT_NAME,
        EngineType::SSI => SSI_DB_ROOT_NAME,
    };
    parent_dir_path.join(db_dir_name)
}
