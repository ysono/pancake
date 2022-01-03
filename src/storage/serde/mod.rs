//! # Serialization format
//!
//! Following pseudocode depicts the byte representation on disk.
//! In-memory representations look similar but are distinct from this.
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
//!     // whether an Datum is a key or a value.
//! }
//! ```
//!
//! `Datum` comes in several varieties.
//!
//! They all start with `datum_type`, which is encoded in `u8`.
//! In case we need to deprecate supported datum_types over time, this allows us
//! `(pow(2, 8) - count_of_active_datum_types)` deprecations, before rolling over to zero.
//!
//! ```text
//! struct OptDatum::Tombstone {
//!     datum_type:     DatumTypeInt(u8),
//! }
//!
//! struct Datum::I64 {
//!     datum_type:     DatumTypeInt(u8),
//!     datum_body:     [u8; 4],
//! }
//!
//! struct Datum::Bytes or Datum::Str {
//!     datum_type:     DatumTypeInt(u8),
//!     datum_body_len: DatumBodyLen(usize),
//!     datum_body:     [u8; datum_body_len],
//! }
//! ```
//!
//! A `Tuple`-typed `Datum` nests other `Datum`s, including possibly other `Tuple`-typed `Datum`s.
//!
//! ```text
//! struct Datum::Tuple {
//!     datum_type:     DatumTypeInt(u8),
//!     datum_body_len: DatumBodyLen(usize),
//!     datum_body:     TupleBody {
//!         members_count:  MembersCount(usize),
//!         member_0:       Datum::I64 {
//!             datum_type:     DatumTypeInt(u8),
//!             datum_body:     [u8; 4],
//!         },
//!         member_1:       Datum::Bytes or Datum::Str {
//!             datum_type:     DatumTypeInt(u8),
//!             datum_body_len: DatumBodyLen(usize),
//!             datum_body:     [u8; datum_body_len],
//!         },
//!         member_2:       Datum::Tuple {
//!             datum_type:     DatumTypeInt(u8),
//!             datum_body: TupleBody {
//!                 members_count:  MembersCount(usize),
//!                 member_0:       Datum::*,
//!                 ...
//!             }
//!         },
//!         ...
//!         // Tombstone may not be nested under Tuple.
//!     }
//! }
//! ```
//!
//! A `Datum` can be skipped over without being deserialized.
//! The amount of bytes to skip is derived dynamically according to `datum_type`.

mod datum_type;
mod iter;
mod lengths;
mod primitives;
mod serde_reader;
mod serde_writer;
mod serializable;

#[cfg(test)]
mod serde_rw_test;

pub use datum_type::*;
pub use iter::*;
pub use lengths::*;
pub use primitives::*;
pub use serde_reader::*;
pub use serde_writer::*;
pub use serializable::*;
