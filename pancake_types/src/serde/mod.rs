//! # Serialization format
//!
//! The primitive de/serializable types are [`OptDatum`] and [`Datum`].
//!
//! The below pseudocode depicts their serialized representations.
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

mod datum;
mod datum_type;
mod lengths;

pub use datum::*;
pub use datum_type::*;
use lengths::*;
