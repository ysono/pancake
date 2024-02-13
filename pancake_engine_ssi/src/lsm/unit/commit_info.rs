use anyhow::{anyhow, Result};
use derive_more::{Deref, DerefMut, From};
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::{FromPrimitive, ToPrimitive};
use pancake_engine_common::fs_utils;
use shorthand::ShortHand;
use std::any;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

#[derive(Deref, DerefMut, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CommitVer(u64);

pub const COMMIT_VER_INITIAL: CommitVer = CommitVer(0);

#[derive(From, Deref, DerefMut, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimestampNum(u64);

#[derive(FromPrimitive, ToPrimitive, PartialEq, Eq)]
pub enum CommitDataType {
    MemLog = 0,
    SSTable = 1,
}

#[derive(PartialEq, Eq, ShortHand)]
pub struct CommitInfo {
    pub commit_ver_hi_incl: CommitVer,
    pub commit_ver_lo_incl: CommitVer,
    pub timestamp_num: TimestampNum,
    pub data_type: CommitDataType,
}

impl CommitInfo {
    fn do_ser<W: Write>(&self, w: &mut BufWriter<W>) -> Result<()> {
        write!(
            w,
            "{},{},{},{}",
            self.commit_ver_hi_incl.0,
            self.commit_ver_lo_incl.0,
            self.timestamp_num.0,
            self.data_type.to_u8().unwrap(),
        )?;
        Ok(())
    }
    fn do_deser<R: Read>(r: &mut BufReader<R>) -> Result<Self> {
        let mut s = String::new();
        r.read_to_string(&mut s)?;

        let tokens = s.split(',').collect::<Vec<&str>>();
        match tokens.try_into() as Result<[&str; 4], _> {
            Err(_) => Err(anyhow!(
                "Incorrect format for {}.",
                any::type_name::<Self>()
            )),
            Ok([hi, lo, ts, typ]) => {
                let hi = hi
                    .parse::<u64>()
                    .map_err(|_| anyhow!("Invalid commit_ver_hi_incl"))?;
                let lo = lo
                    .parse::<u64>()
                    .map_err(|_| anyhow!("Invalid commit_ver_lo_incl"))?;
                let ts = ts
                    .parse::<u64>()
                    .map_err(|_| anyhow!("Invalid timestamp_num"))?;
                let typ = typ
                    .parse::<u8>()
                    .map_err(|_| anyhow!("Invalid data_type"))?;
                let typ = match FromPrimitive::from_u8(typ) {
                    Some(CommitDataType::MemLog) => CommitDataType::MemLog,
                    Some(CommitDataType::SSTable) => CommitDataType::SSTable,
                    None => return Err(anyhow!("Invalid data_type")),
                };
                Ok(Self {
                    commit_ver_hi_incl: CommitVer(hi),
                    commit_ver_lo_incl: CommitVer(lo),
                    timestamp_num: TimestampNum(ts),
                    data_type: typ,
                })
            }
        }
    }
    pub fn ser<P: AsRef<Path>>(&self, p: P) -> Result<()> {
        let file = fs_utils::open_file(p, OpenOptions::new().create(true).write(true))?;
        let mut w = BufWriter::new(file);
        self.do_ser(&mut w)?;
        w.flush()?;
        Ok(())
    }
    pub fn deser<P: AsRef<Path>>(p: P) -> Result<Self> {
        let file = fs_utils::open_file(p, OpenOptions::new().read(true))?;
        let mut r = BufReader::new(file);
        Self::do_deser(&mut r)
    }
}

impl PartialOrd for CommitInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let commit_ver_ord = self.commit_ver_hi_incl.cmp(&other.commit_ver_hi_incl);
        let ord = commit_ver_ord.then_with(|| self.timestamp_num.cmp(&other.timestamp_num));
        Some(ord)
    }
}
impl Ord for CommitInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
