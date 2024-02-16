use anyhow::{anyhow, Result};
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::{FromPrimitive, ToPrimitive};
use pancake_engine_common::fs_utils;
use shorthand::ShortHand;
use std::any;
use std::cmp::{self, Ord, PartialOrd};
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

/// The commit version uniquely identifies every commitment as well as the datastore state after the commitment.
///
/// The datastore's commit version increases for the whole lifetime of the datastore instance.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CommitVer(u64);

impl CommitVer {
    pub const AT_EMPTY_DATASTORE: Self = Self(0);

    pub fn inc(self) -> Self {
        Self(self.0 + 1)
    }
}

/// The ordered number that disambiguates 2+ [`CommitInfo`] instances.
///
/// In case 2+ [`CommitInfo`]s overlap in their [`CommitVer`] intervals,
/// this number indicates the new-ness of each [`CommitInfo`].
///
/// When loading a collection of overlapping [`CommitInfo`]s,
/// all among them with the largest [`ReplacementNum`] should be retained,
/// and all other others should be ignored and discarded.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReplacementNum(u64);

impl ReplacementNum {
    pub const FOR_NEW_COMMIT_VER_INTERVAL: Self = Self(0);

    pub fn max(mut vals: impl Iterator<Item = Self>) -> Option<Self> {
        let mut max_val = vals.next();
        for val in vals {
            max_val = Some(cmp::max(max_val.unwrap(), val));
        }
        max_val
    }
    pub fn new_larger_than_all_of(vals: impl Iterator<Item = Self>) -> Self {
        match Self::max(vals) {
            None => Self(0),
            Some(val) => Self(val.0 + 1),
        }
    }
}

/// The disambiguator of key-value files on disk, as to whether belonging to a MemLog or an SSTable.
#[derive(FromPrimitive, ToPrimitive, PartialEq, Eq)]
pub enum CommitDataType {
    MemLog = 0,
    SSTable = 1,
}

#[derive(PartialEq, Eq, ShortHand)]
pub struct CommitInfo {
    pub commit_ver_hi_incl: CommitVer,
    pub commit_ver_lo_incl: CommitVer,
    pub replacement_num: ReplacementNum,
    pub data_type: CommitDataType,
}

impl CommitInfo {
    fn do_ser<W: Write>(&self, w: &mut BufWriter<W>) -> Result<()> {
        write!(
            w,
            "{},{},{},{}",
            self.commit_ver_hi_incl.0,
            self.commit_ver_lo_incl.0,
            self.replacement_num.0,
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
            Ok([hi, lo, replc_num, typ]) => {
                let hi = hi
                    .parse::<u64>()
                    .map_err(|_| anyhow!("Invalid commit_ver_hi_incl"))?;
                let lo = lo
                    .parse::<u64>()
                    .map_err(|_| anyhow!("Invalid commit_ver_lo_incl"))?;
                let replc_num = replc_num
                    .parse::<u64>()
                    .map_err(|_| anyhow!("Invalid replacement_num"))?;
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
                    replacement_num: ReplacementNum(replc_num),
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
