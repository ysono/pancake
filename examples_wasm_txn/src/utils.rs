use anyhow::Result;
use pancake_types::types::{Deser, PrimaryKey, Value};

pub fn pkpv_to_string(pk: &[u8], pv: &[u8]) -> Result<String, String> {
    let pk = PrimaryKey::deser_solo(pk).map_err(|e| e.to_string());
    let pv = Value::deser_solo(pv).map_err(|e| e.to_string());
    let s = format!("Key:\r\n{:?}\r\nValue:\r\n{:?}\r\n", pk, pv);
    Ok(s)
}
