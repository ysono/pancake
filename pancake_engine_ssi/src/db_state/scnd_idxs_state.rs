use anyhow::{anyhow, Result};
use pancake_engine_common::fs_utils::{self, PathNameNum};
use pancake_types::{io_utils, types::SubValueSpec};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Cursor, Read, Write};
use std::path::Path;
use std::str;
use std::sync::Arc;

mod test;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ScndIdxNum(u64);

impl From<PathNameNum> for ScndIdxNum {
    fn from(num: PathNameNum) -> Self {
        Self(*num)
    }
}
impl Into<PathNameNum> for ScndIdxNum {
    fn into(self) -> PathNameNum {
        PathNameNum::from(self.0)
    }
}
impl ScndIdxNum {
    pub const AT_EMPTY_DATASTORE: Self = Self(0);

    pub fn fetch_inc(&mut self) -> Self {
        let ret = Self(self.0);
        self.0 += 1;
        ret
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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
        let mut buf = vec![];

        /* scnd_idx_num */
        io_utils::read_until_then_trim(r, ',' as u8, &mut buf)?;
        if buf.is_empty() {
            return Ok(None);
        }
        let scnd_idx_num = str::from_utf8(&buf)?
            .parse::<u64>()
            .map_err(|_| anyhow!("Invalid scnd_idx_num"))?;

        /* is_readable */
        buf.clear();
        io_utils::read_until_then_trim(r, '\n' as u8, &mut buf)?;
        let is_readable_str = str::from_utf8(&buf)?;
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

#[derive(PartialEq, Eq, Debug)]
pub struct ScndIdxsState {
    pub(super) scnd_idxs: HashMap<Arc<SubValueSpec>, ScndIdxState>,
    pub(super) next_scnd_idx_num: ScndIdxNum,
}

impl ScndIdxsState {
    pub fn new_empty() -> Self {
        Self {
            scnd_idxs: Default::default(),
            next_scnd_idx_num: ScndIdxNum::AT_EMPTY_DATASTORE,
        }
    }

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
        let mut buf = vec![];

        /* next_scnd_idx_num */
        io_utils::read_until_then_trim(r, '\n' as u8, &mut buf)?;
        let next_scnd_idx_num = str::from_utf8(&buf)?
            .parse::<u64>()
            .map_err(|_| anyhow!("Invalid next_scnd_idx_num"))?;
        let next_scnd_idx_num = ScndIdxNum(next_scnd_idx_num);

        let mut scnd_idxs = HashMap::new();
        loop {
            buf.clear();

            /* sv_spec */
            io_utils::read_until_then_trim(r, '\n' as u8, &mut buf)?;
            if buf.is_empty() {
                break;
            }
            let mut line_reader = BufReader::new(Cursor::new(&buf));
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
        let file = fs_utils::open_file(path, OpenOptions::new().create(true).write(true))?;
        let mut w = BufWriter::new(file);
        self.do_ser(&mut w)?;
        w.flush()?;
        Ok(())
    }
    pub fn deser<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = fs_utils::open_file(path, OpenOptions::new().read(true))?;
        let mut r = BufReader::new(file);
        Self::do_deser(&mut r)
    }
}
