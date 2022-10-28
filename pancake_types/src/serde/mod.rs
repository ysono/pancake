//! # Serialization format
//!
//! Following pseudocode depicts the byte representation on disk.
//! In-memory representations are distinct from this.
//!
//! This file format is applicable for both commit logs and ss tables.
//!
//! ```text
//! struct File {
//!     k0: Datum,
//!     v0: Datum,
//!     k1: Datum,
//!     v1: Datum,
//!     ...
//!     // There are no separators in between Datums and nothing to indicate
//!     // whether a Datum is a key or a value.
//! }
//! ```
//!
//! `Datum` comes in several varieties.
//!
//! They all start with `datum_type`, which is encoded in `u8`.
//! In case we need to deprecate supported datum_types over time, this allows us
//! `(pow(2, 8) - count_of_active_datum_types)` deprecations, before rolling over to zero.
//!
//! Some `Datum` types have fixed body lengths; these lengths are not encoded.
//! For other `Datum` types, which have dynamic body lengths, these lengths are
//! encoded following `datum_type`.
//! Readers may skip the body.
//!
//! A `Datum::Tuple` nests other non-`Tombstone` `Datum`s, including possibly other `Datum::Tuple`s.
//!
//! ```text
//! struct OptDatum::Tombstone {
//!     datum_type:     u8,
//! }
//!
//! struct Datum::I64 {
//!     datum_type:     u8,
//!     datum_body:     [u8; 8],
//! }
//!
//! struct Datum::Bytes or Datum::Str {
//!     datum_type:         u8,
//!     datum_body_len:     u32,
//!     datum_body:         [u8; datum_body_len],
//! }
//!
//! struct Datum::Tuple {
//!     datum_type:         u8,
//!     datum_body_len:     u32,
//!     datum_body:         {
//!         members_count:      u32,
//!         member_0:           Datum::I64 {
//!             datum_type:         u8,
//!             datum_body:         [u8; 8],
//!         },
//!         member_1:           Datum::Bytes or Datum::Str {
//!             datum_type:         u8,
//!             datum_body_len:     u32,
//!             datum_body:         [u8; datum_body_len],
//!         },
//!         member_2:           Datum::Tuple {
//!             datum_type:         u8,
//!             // (Notice, no datum_body_len here.)
//!             datum_body:         {
//!                 members_count:      u32,
//!                 member_0:           Datum::*,
//!                 ...
//!             }
//!         },
//!         member_3:           Datum::*
//!         ...
//!         // Tombstone may not be nested under Tuple.
//!     }
//! }
//! ```

mod iter_k;
mod iter_kv;
mod iter_range;
mod lengths;

mod datum_type;
mod primitives;
mod serde_reader;
mod serde_writer;
mod serializable;

#[cfg(test)]
mod serde_rw_test;

pub use iter_k::*;
pub use iter_kv::*;
pub use iter_range::*;
pub use lengths::*;

pub use datum_type::*;
pub use primitives::*;
pub use serde_reader::*;
pub use serde_writer::*;
pub use serializable::*;
