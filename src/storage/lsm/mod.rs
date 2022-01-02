//! An LSM-Tree is an abstraction of a sorted key-value dictionary.
//!
//! ### API:
//!
//! The exposed operations are: `put one`, `get one`, `get range`.
//!
//! Values are immutable. They cannot be modified in-place, and must be replaced.
//!
//! ### Internals:
//!
//! An in-memory sorted structure holds the most recently inserted `{key: value}` mapping.
//!
//! The in-memory structure is occasionally flushed into an SSTable.
//!
//! Multiple SSTables are occasionally compacted into one SSTable.
//!
//! ![](https://user-images.githubusercontent.com/5148696/128642691-55eea319-05a4-43bf-a2f9-13e9f5132a74.png)
//!
//! ### Querying:
//!
//! A `put` operation accesses the in-memory head structure only.
//!
//! A `get` operation generally accesses the in-memory head and all SSTables.
//!
//! When the same key exists in multiple sources, only the result from the newest source is retrieved.
//!
//! ![](https://user-images.githubusercontent.com/5148696/128660102-e6da6e45-b6a1-4a2b-b038-66af51f212c7.png)

mod entry;
mod lsm_tree;
mod memlog;
pub mod merging;
mod sstable;

use memlog::*;
use sstable::*;

pub use entry::Entry;
pub use lsm_tree::LSMTree;
