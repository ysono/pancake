use crate::storage::types::SubValueSpec;
use anyhow::{anyhow, Result};
use derive_more::{Deref, From};
use std::any;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Cursor, Read, Write};
use std::path::Path;
use std::str;
use std::sync::Arc;

fn read_line_trimmed<R: Read>(r: &mut BufReader<R>) -> Result<Vec<u8>> {
    let mut buf = vec![];
    r.read_until('\n' as u8, &mut buf)?;
    if buf.last() == Some(&('\n' as u8)) {
        buf.pop();
    }
    Ok(buf)
}

#[derive(Default, From, Deref, Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ScndIdxNum(u64);

impl ScndIdxNum {
    pub fn get_and_inc(&mut self) -> Self {
        let ret = Self(self.0);
        self.0 += 1;
        ret
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ScndIdxState {
    pub scnd_idx_num: ScndIdxNum,
    pub is_readable: bool,
}

impl ScndIdxState {
    fn do_ser<W: Write>(&self, w: &mut BufWriter<W>) -> Result<()> {
        write!(
            w,
            "{},{}\n",
            self.scnd_idx_num.0,
            if self.is_readable { 'T' } else { 'F' }
        )?;
        Ok(())
    }
    fn do_deser<R: Read>(r: &mut BufReader<R>) -> Result<Option<Self>> {
        let line = read_line_trimmed(r)?;
        if line.len() == 0 {
            return Ok(None);
        }
        let line = str::from_utf8(&line)?;
        let tokens = line.split(',').collect::<Vec<&str>>();
        match tokens.try_into() as Result<[&str; 2], _> {
            Err(_) => Err(anyhow!(
                "Incorrect format for {}.",
                any::type_name::<Self>()
            )),
            Ok([scnd_idx_num, is_readable_str]) => {
                let scnd_idx_num = scnd_idx_num
                    .parse::<u64>()
                    .map_err(|_| anyhow!("Invalid scnd_idx_num"))?;

                let is_readable = if is_readable_str == "T" {
                    true
                } else if is_readable_str == "F" {
                    false
                } else {
                    return Err(anyhow!("Invalid is_readable"));
                };

                Ok(Some(Self {
                    scnd_idx_num: ScndIdxNum(scnd_idx_num),
                    is_readable,
                }))
            }
        }
    }
}

#[derive(Default, PartialEq, Eq, Debug)]
pub struct ScndIdxsState {
    pub(super) scnd_idxs: HashMap<Arc<SubValueSpec>, ScndIdxState>,
    pub(super) next_scnd_idx_num: ScndIdxNum,
}

impl ScndIdxsState {
    fn do_ser<W: Write>(&self, w: &mut BufWriter<W>) -> Result<()> {
        /* next_scnd_idx_num */
        write!(w, "{}\n", self.next_scnd_idx_num.0)?;

        for (sv_spec, scnd_idx_state) in self.scnd_idxs.iter() {
            /* sv_spec */
            sv_spec.ser(w)?;
            write!(w, "\n")?;

            /* scnd_idx_state */
            scnd_idx_state.do_ser(w)?;
        }

        Ok(())
    }
    fn do_deser<R: Read>(r: &mut BufReader<R>) -> Result<Self> {
        /* next_scnd_idx_num */
        let line = read_line_trimmed(r)?;
        let line = str::from_utf8(&line)?;
        let next_scnd_idx_num = line
            .parse::<u64>()
            .map_err(|_| anyhow!("Invalid next_scnd_idx_num"))?;
        let next_scnd_idx_num = ScndIdxNum::from(next_scnd_idx_num);

        let mut scnd_idxs = HashMap::new();
        loop {
            /* sv_spec */
            let line = read_line_trimmed(r)?;
            if line.len() == 0 {
                break;
            }
            let mut line_reader = BufReader::new(Cursor::new(line));
            let sv_spec = SubValueSpec::deser(&mut line_reader)?;

            /* scnd_idx_state */
            let scnd_idx_state = ScndIdxState::do_deser(r)?
                .ok_or_else(|| anyhow!("sv_spec without scnd_idx_state."))?;

            scnd_idxs.insert(Arc::new(sv_spec), scnd_idx_state);
        }

        Ok(Self {
            scnd_idxs,
            next_scnd_idx_num,
        })
    }
    pub fn ser<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(path.as_ref())?;
        let mut w = BufWriter::new(file);
        self.do_ser(&mut w)?;
        w.flush()?;
        Ok(())
    }
    pub fn deser<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path.as_ref())?;
        let mut r = BufReader::new(file);
        Self::do_deser(&mut r)
    }
}

#[cfg(test)]
mod test;
