//! Iterators work on files that contain "K"s and "V"s interleaved,
//! where "K" and "V" are some [`crate::types::Deser`] types.
//! There is no separator between neighboring "K" <-> "V", and
//! there is no indication whether the current bytes correspond to "K" or "V".
//!
//! This file format is applicable to both commit logs and SS tables.
//!
//! ```text
//! struct File {
//!     k0: K,
//!     v0: V,
//!     k1: K,
//!     v1: V,
//!     ...
//!     // where K: Deser
//!     //   and V: Deser
//! }
//! ```

mod iter_k;
mod iter_kv;
mod iter_range;

pub use iter_k::*;
pub use iter_kv::*;
pub use iter_range::*;
