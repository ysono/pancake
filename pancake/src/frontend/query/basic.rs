//! A _very_ basic parser
//!
//! # Supported queries
//!
//! ## By primary key
//!
//! Keys and values are typed.
//!
//! - `put int(100) str(1000)`
//! - `del int(100)`
//! - `get int(100)`
//!
//! The tuple type nests other data, including other tuples.
//!
//! - `put int(6000) tup( str(s6000) tup( int(60) str(s60) ) int(60) )`
//! - `get tup( str(a) int(10) )`
//!
//! ## By range over primary key
//!
//! Analogous sql:
//!
//! - `SELECT * FROM table WHERE pk BETWEEN ${pk_lo} AND ${pk_hi};`
//! - `SELECT * FROM table WHERE pk <= ${pk_hi};`
//!
//! Only inclusive boundaries are supported.
//!
//! - `get between int(50) str(foobar)`
//! - `get between int(50) _`
//! - `get between _ str(foobar)`
//! - `get between _ _`
//!
//! ## By sub-portion of value
//!
//! ### Index creation
//!
//! Analogous sql:
//!
//! `CREATE INDEX ON table (${column});`
//!
//! Whereas a RDBMS allows specifing an index based on
//! a selection of one or more columns, we support a selection of any of:
//!
//! - The whole value
//! - One contiguous sub-portion of value at a specific nested location and having a specific type
//!
//! Index all entries by value type.
//!
//! `create index int`
//!
//! Index all entries by sub-value specification.
//!
//! `create index nested( 0 str )`
//!
//! Index all entries by nested sub-value specification.
//!
//! `create index nested( 1 0 int )`
//!
//! ### Index-based selection
//!
//! Analogous sql:
//!
//! - `SELECT * FROM table WHERE ${column} = ${col_val};`
//! - `SELECT * FROM table WHERE ${column} BETWEEN ${col_val_lo} AND ${col_val_hi};`
//! - `SELECT * FROM table WHERE ${column} <= ${col_val_hi};`
//!
//! In addition, because value schemas are dynamic, we also support selecting all values
//! that match a spec, regardless of the sub-portion of value pointed to by the spec.
//! It would be analogous to this hypothetical sql:
//!
//! - `SELECT * FROM table WHERE ${column} IS VALID COLUMN;`
//!
//! Get all entries by value type.
//!
//! - `get where int int(1000)`
//! - `get where int between int(500) int(1500)`
//! - `get where int between _ int(1500)`
//! - `get where int _`
//!
//! Get all entries by sub-value specification.
//!
//! - `get where nested( 0 str ) str(s6000)`
//! - `get where nested( 0 str ) between str(s1000) str(s9000)`
//! - `get where nested( 0 str ) _`
//!
//! Get all entries by nested sub-value specification.
//!
//! - `get where nested( 1 0 int ) int(60)`
//! - `get where nested( 1 0 int ) between int(60) int(61)`
//! - `get where nested( 1 0 int ) _`
//!
//! # Caveats
//!
//! The input string is split by unicode word boundary. This incurs some limitations:
//! - Literals such as `foo.bar` and `foo-bar` are separated into multiple tokens.
//!     - This means any data containing such characters as `.` and `-` are unworkable with this query engine.
//! - Literals such as `("` and `))` are not separated.
//!     - Hence, when in doubt, add spaces.
//!
//! This is a simplistic, recursion-based parser.
//! It's meant to be a stop-gap impl.
//! It ought to be replaced by one based on a lexer and a parser.

use crate::frontend::api::{Operation, SearchRange, Statement};
use crate::storage::serde::{Datum, DatumType};
use crate::storage::types::{PrimaryKey, SubValue, SubValueSpec, Value};
use anyhow::{anyhow, Context, Result};
use regex::Regex;
use std::iter::Peekable;

pub fn parse(q_str: &str) -> Result<Operation> {
    let reg = Regex::new(r"\s+|\b").unwrap();
    let iter = reg.split(q_str).filter(|w| w.len() > 0).peekable();
    root(iter)
}

fn root<'a, I: Iterator<Item = &'a str>>(mut iter: Peekable<I>) -> Result<Operation> {
    match iter.next() {
        Some("put") => {
            let dat = datum(&mut iter)?;
            let key = PrimaryKey(dat);
            let dat = datum(&mut iter)?;
            let val = Value(dat);
            eos(&mut iter)?;

            let q = Operation::from(Statement::Put(key, Some(val)));
            return Ok(q);
        }
        Some("del") => {
            let dat = datum(&mut iter)?;
            eos(&mut iter)?;

            let key = PrimaryKey(dat);
            let q = Operation::from(Statement::Put(key, None));
            return Ok(q);
        }
        Some("get") => match iter.peek() {
            Some(&"between") => {
                iter.next();

                let optdat = opt_datum(&mut iter)?;
                let pk_lo = optdat.map(PrimaryKey);
                let optdat = opt_datum(&mut iter)?;
                let pk_hi = optdat.map(PrimaryKey);
                eos(&mut iter)?;

                let q = Operation::from(Statement::GetPK(SearchRange::Range {
                    lo: pk_lo,
                    hi: pk_hi,
                }));
                return Ok(q);
            }
            Some(&"where") => {
                iter.next();

                let spec = subvalspec(&mut iter)?;

                match iter.peek() {
                    Some(&"between") => {
                        iter.next();

                        let optdat = opt_datum(&mut iter)?;
                        let sv_lo = optdat.map(SubValue);
                        let optdat = opt_datum(&mut iter)?;
                        let sv_hi = optdat.map(SubValue);
                        eos(&mut iter)?;

                        let q = Operation::from(Statement::GetSV(
                            spec,
                            SearchRange::Range {
                                lo: sv_lo,
                                hi: sv_hi,
                            },
                        ));
                        return Ok(q);
                    }
                    _ => {
                        let optdat = opt_datum(&mut iter)?;
                        eos(&mut iter)?;

                        match optdat {
                            None => {
                                let q = Operation::from(Statement::GetSV(spec, SearchRange::all()));
                                return Ok(q);
                            }
                            Some(dat) => {
                                let sv = SubValue(dat);
                                let q =
                                    Operation::from(Statement::GetSV(spec, SearchRange::One(sv)));
                                return Ok(q);
                            }
                        }
                    }
                }
            }
            _ => {
                let dat = datum(&mut iter)?;
                let key = PrimaryKey(dat);
                eos(&mut iter)?;

                let q = Operation::from(Statement::GetPK(SearchRange::One(key)));
                return Ok(q);
            }
        },
        Some("create") => match iter.next() {
            Some("index") => {
                let spec = subvalspec(&mut iter)?;
                eos(&mut iter)?;
                return Ok(Operation::CreateScndIdx(spec));
            }
            x => return Err(anyhow!("Expected creatable but found {:?}", x)),
        },
        x => return Err(anyhow!("Expected operation but found {:?}", x)),
    }
}

fn datum<'a, I: Iterator<Item = &'a str>>(iter: &mut Peekable<I>) -> Result<Datum> {
    match iter.next() {
        Some("str") => match iter.next() {
            Some("(") => match iter.next() {
                Some(str_literal) => match iter.next() {
                    Some(")") => return Ok(Datum::Str(String::from(str_literal))),
                    x => {
                        return Err(anyhow!(
                            "Expected closing of string literal but found {:?}",
                            x
                        ))
                    }
                },
                None => return Err(anyhow!("Expected string literal but found EOS")),
            },
            x => {
                return Err(anyhow!(
                    "Expected opening of string literal but found {:?}",
                    x
                ))
            }
        },
        Some("int") => match iter.next() {
            Some("(") => match iter.next() {
                Some(int_literal) => {
                    let int_val = int_literal
                        .parse::<i64>()
                        .context(format!("Expected i64 literal but found {}", int_literal))?;
                    match iter.next() {
                        Some(")") => return Ok(Datum::I64(int_val)),
                        x => {
                            return Err(anyhow!(
                                "Expected closing of int literal but found {:?}",
                                x
                            ))
                        }
                    }
                }
                None => return Err(anyhow!("Expected int literal but found EOS")),
            },
            x => return Err(anyhow!("Expected opening of int literal but found {:?}", x)),
        },
        Some("tup") => match iter.next() {
            Some("(") => {
                let mut members = Vec::<Datum>::new();
                loop {
                    match iter.peek() {
                        Some(&")") => {
                            iter.next();
                            return Ok(Datum::Tuple(members));
                        }
                        _ => {
                            let member = datum(iter)?;
                            members.push(member);
                        }
                    }
                }
            }
            x => return Err(anyhow!("Expected opening of tuple but found {:?}", x)),
        },
        x => Err(anyhow!("Expected datum type but found {:?}", x)),
    }
}

fn subvalspec<'a, I: Iterator<Item = &'a str>>(iter: &mut I) -> Result<SubValueSpec> {
    match iter.next() {
        Some("str") => return Ok(SubValueSpec::whole(DatumType::Str)),
        Some("int") => return Ok(SubValueSpec::whole(DatumType::I64)),
        Some("nested") => match iter.next() {
            Some("(") => {
                let mut member_idxs = vec![];
                let mut datum_type = None;
                loop {
                    match iter.next() {
                        Some(")") => break,
                        Some(token) => {
                            if datum_type.is_some() {
                                return Err(anyhow!("Nested subvalspec's datum_type is followed by an extra token {}.", token));
                            }
                            if token == "str" {
                                datum_type = Some(DatumType::Str);
                            } else if token == "int" {
                                datum_type = Some(DatumType::I64);
                            } else {
                                let member_idx = token.parse::<usize>().context(format!(
                                    "Expected nested subvalspec member_idx but found {}.",
                                    token
                                ))?;
                                member_idxs.push(member_idx);
                            }
                        }
                        None => {
                            return Err(anyhow!("Expected nested subvalspec defn but found EOS"))
                        }
                    }
                }
                match datum_type {
                    None => {
                        return Err(anyhow!(
                            "Nested subvalspec defn did not contain datum_type."
                        ))
                    }
                    Some(datum_type) => {
                        return Ok(SubValueSpec {
                            member_idxs,
                            datum_type,
                        })
                    }
                }
            }
            x => {
                return Err(anyhow!(
                    "Expected opening of nested subvalspec but found {:?}",
                    x
                ))
            }
        },
        x => return Err(anyhow!("Expected opening of subvalspec but found {:?}", x)),
    }
}

fn opt_datum<'a, I: Iterator<Item = &'a str>>(iter: &mut Peekable<I>) -> Result<Option<Datum>> {
    match iter.peek() {
        Some(&"_") => {
            iter.next();
            return Ok(None);
        }
        _ => {
            let dat = datum(iter)?;
            return Ok(Some(dat));
        }
    }
}

fn eos<'a, I: Iterator<Item = &'a str>>(iter: &mut I) -> Result<()> {
    match iter.next() {
        None => Ok(()),
        x => Err(anyhow!("Expected EOS but found {:?}", x)),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn put() -> Result<()> {
        let q_str = "put int(123) str(val1)";
        let exp_q_obj = Operation::from(Statement::Put(
            PrimaryKey(Datum::I64(123)),
            Some(Value(Datum::Str(String::from("val1")))),
        ));
        assert!(parse(q_str)? == exp_q_obj);

        let q_str = "put tup( str(a) int(123) ) int(321)";
        let exp_q_obj = Operation::from(Statement::Put(
            PrimaryKey(Datum::Tuple(vec![
                Datum::Str(String::from("a")),
                Datum::I64(123),
            ])),
            Some(Value(Datum::I64(321))),
        ));
        assert!(parse(q_str)? == exp_q_obj);

        Ok(())
    }

    #[test]
    fn del() -> Result<()> {
        let q_str = "del int(123)";
        let exp_q_obj = Operation::from(Statement::Put(PrimaryKey(Datum::I64(123)), None));
        assert!(parse(q_str)? == exp_q_obj);

        Ok(())
    }

    #[test]
    fn get() -> Result<()> {
        let q_str = "get int(123)";
        let exp_q_obj = Operation::from(Statement::GetPK(SearchRange::One(PrimaryKey(
            Datum::I64(123),
        ))));
        assert!(parse(q_str)? == exp_q_obj);

        let q_str = "get str(key1)";
        let exp_q_obj = Operation::from(Statement::GetPK(SearchRange::One(PrimaryKey(
            Datum::Str(String::from("key1")),
        ))));
        assert!(parse(q_str)? == exp_q_obj);

        let q_str = "get tup( str(a) int(123) )";
        let exp_q_obj = Operation::from(Statement::GetPK(SearchRange::One(PrimaryKey(
            Datum::Tuple(vec![Datum::Str(String::from("a")), Datum::I64(123)]),
        ))));
        assert!(parse(q_str)? == exp_q_obj);

        Ok(())
    }

    #[test]
    fn get_between() -> Result<()> {
        let q_str = "get between int(123) int(234)";
        let exp_q_obj = Operation::from(Statement::GetPK(SearchRange::Range {
            lo: Some(PrimaryKey(Datum::I64(123))),
            hi: Some(PrimaryKey(Datum::I64(234))),
        }));
        assert!(parse(q_str)? == exp_q_obj);

        let q_str = "get between int(123) _";
        let exp_q_obj = Operation::from(Statement::GetPK(SearchRange::Range {
            lo: Some(PrimaryKey(Datum::I64(123))),
            hi: None,
        }));
        assert!(parse(q_str)? == exp_q_obj);

        let q_str = "get between _ int(234)";
        let exp_q_obj = Operation::from(Statement::GetPK(SearchRange::Range {
            lo: None,
            hi: Some(PrimaryKey(Datum::I64(234))),
        }));
        assert!(parse(q_str)? == exp_q_obj);

        let q_str = "get between _ _";
        let exp_q_obj =
            Operation::from(Statement::GetPK(SearchRange::Range { lo: None, hi: None }));
        assert!(parse(q_str)? == exp_q_obj);

        Ok(())
    }

    #[test]
    fn get_where() -> Result<()> {
        let q_str = "get where int _";
        let exp_q_obj = Operation::from(Statement::GetSV(
            SubValueSpec::whole(DatumType::I64),
            SearchRange::all(),
        ));
        assert!(parse(q_str)? == exp_q_obj);

        let q_str = "get where int int(123)";
        let exp_q_obj = Operation::from(Statement::GetSV(
            SubValueSpec::whole(DatumType::I64),
            SearchRange::One(SubValue(Datum::I64(123))),
        ));
        assert!(parse(q_str)? == exp_q_obj);

        let q_str = "get where nested( 1 0 str ) str(subval_a)";
        let exp_q_obj = Operation::from(Statement::GetSV(
            SubValueSpec {
                member_idxs: vec![1, 0],
                datum_type: DatumType::Str,
            },
            SearchRange::One(SubValue(Datum::Str(String::from("subval_a")))),
        ));
        assert!(parse(q_str)? == exp_q_obj);

        Ok(())
    }

    #[test]
    fn get_where_between() -> Result<()> {
        let q_str = "get where int between int(123) int(234)";
        let exp_q_obj = Operation::from(Statement::GetSV(
            SubValueSpec::whole(DatumType::I64),
            SearchRange::Range {
                lo: Some(SubValue(Datum::I64(123))),
                hi: Some(SubValue(Datum::I64(234))),
            },
        ));
        assert!(parse(q_str)? == exp_q_obj);

        let q_str = "get where int between int(123) _";
        let exp_q_obj = Operation::from(Statement::GetSV(
            SubValueSpec::whole(DatumType::I64),
            SearchRange::Range {
                lo: Some(SubValue(Datum::I64(123))),
                hi: None,
            },
        ));
        assert!(parse(q_str)? == exp_q_obj);

        let q_str = "get where int between _ int(234)";
        let exp_q_obj = Operation::from(Statement::GetSV(
            SubValueSpec::whole(DatumType::I64),
            SearchRange::Range {
                lo: None,
                hi: Some(SubValue(Datum::I64(234))),
            },
        ));
        assert!(parse(q_str)? == exp_q_obj);

        let q_str = "get where int between _ _";
        let exp_q_obj = Operation::from(Statement::GetSV(
            SubValueSpec::whole(DatumType::I64),
            SearchRange::all(),
        ));
        assert!(parse(q_str)? == exp_q_obj);

        Ok(())
    }

    #[test]
    fn create_scnd_idx() -> Result<()> {
        let q_str = "create index int";
        let exp_q_obj = Operation::CreateScndIdx(SubValueSpec::whole(DatumType::I64));
        assert!(parse(q_str)? == exp_q_obj);

        let q_str = "create index nested( 2 int )";
        let exp_q_obj = Operation::CreateScndIdx(SubValueSpec {
            member_idxs: vec![2],
            datum_type: DatumType::I64,
        });
        assert!(parse(q_str)? == exp_q_obj);

        let q_str = "create index nested( 1 0 str )";
        let exp_q_obj = Operation::CreateScndIdx(SubValueSpec {
            member_idxs: vec![1, 0],
            datum_type: DatumType::Str,
        });
        assert!(parse(q_str)? == exp_q_obj);

        Ok(())
    }
}
