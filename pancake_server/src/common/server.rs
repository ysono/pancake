use pancake_engine_common::fs_utils::EngineType;

pub const ENV_VAR_ROOT_DIR: &str = "PANCAKE_ROOT_DIR";

pub const ENV_VAR_BIND_ADDR: &str = "PANCAKE_BIND_ADDR";

pub fn default_bind_addr(typ: EngineType) -> &'static str {
    match typ {
        EngineType::SERIAL => "127.0.0.1:3000",
        EngineType::SSI => "127.0.0.1:3001",
    }
}
