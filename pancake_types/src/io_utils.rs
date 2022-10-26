use anyhow::Result;
use std::io::BufRead;

pub fn read_until_then_trim(r: &mut impl BufRead, byte: u8, buf: &mut Vec<u8>) -> Result<()> {
    let r_len = r.read_until(byte, buf)?;
    if r_len > 0 && buf.last() == Some(&byte) {
        buf.pop();
    }
    Ok(())
}
