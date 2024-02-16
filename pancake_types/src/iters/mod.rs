//! Iterators work on files that contain "K"s and "V"s alternately,
//! where "K" and "V" are some [`crate::types::Deser`] types.
//! There is no separator between neighboring "K" <-> "V" <-> "K" <-> etc, and
//! there is no indication whether the current bytes correspond to "K" or "V".
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
//!
//! This file format is applicable to both commit logs and SS tables.

mod iter_range;
mod iters_simple;
mod reader;

pub use iter_range::*;
pub use iters_simple::*;
pub use reader::*;
