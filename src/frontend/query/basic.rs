//! A _very_ basic parser
//!
//! This is a simplistic, recursion-based parser.
//! It's meant to be a stop-gap impl.
//! It ought to be replaced by one based on a lexer and a parser.
//!
//! The input string is split by unicode word boundary. This incurs some limitations:
//! - Literals such as `foo.bar` and `foo-bar` are separated into multiple tokens.
//!     - This means any data containing such characters as `.` and `-` are unworkable with this query engine.
//! - Literals such as `("` and `))` are not separated.
//!     - Hence, when in doubt, add spaces.

use crate::storage::serde::DatumType;
use crate::storage::types::{Datum, PrimaryKey, SubValue, SubValueSpec, Value};
use anyhow::{anyhow, Context, Result};
use regex::Regex;
use std::iter::Peekable;

#[derive(PartialEq, Eq, Debug)]
pub enum Query {
    Put(PrimaryKey, Value),
    Del(PrimaryKey),
    Get(PrimaryKey),
    GetBetween(Option<PrimaryKey>, Option<PrimaryKey>),
    GetWhere(SubValueSpec, Option<SubValue>),
    GetWhereBetween(SubValueSpec, Option<SubValue>, Option<SubValue>),
    CreateSecIdx(SubValueSpec),
}

pub fn parse(q_str: &str) -> Result<Query> {
    let reg = Regex::new(r"\s+|\b").unwrap();
    let iter = reg.split(q_str).filter(|w| w.len() > 0).peekable();
    root(iter)
}

fn root<'a, I: Iterator<Item = &'a str>>(mut iter: Peekable<I>) -> Result<Query> {
    match iter.next() {
        Some("put") => {
            let dat = datum(&mut iter)?;
            let key = PrimaryKey(dat);
            let dat = datum(&mut iter)?;
            let val = Value(dat);
            eos(&mut iter)?;

            let q = Query::Put(key, val);
            return Ok(q);
        }
        Some("del") => {
            let dat = datum(&mut iter)?;
            eos(&mut iter)?;

            let key = PrimaryKey(dat);
            let q = Query::Del(key);
            return Ok(q);
        }
        Some("get") => match iter.peek() {
            Some(&"between") => {
                iter.next();

                let optdat = opt_datum(&mut iter)?;
                let key_lo = optdat.map(PrimaryKey);
                let optdat = opt_datum(&mut iter)?;
                let key_hi = optdat.map(PrimaryKey);
                eos(&mut iter)?;

                let q = Query::GetBetween(key_lo, key_hi);
                return Ok(q);
            }
            Some(&"where") => {
                iter.next();

                let spec = subvalspec(&mut iter)?;

                match iter.peek() {
                    Some(&"between") => {
                        iter.next();

                        let optdat = opt_datum(&mut iter)?;
                        let subval_lo = optdat.map(SubValue);
                        let optdat = opt_datum(&mut iter)?;
                        let subval_hi = optdat.map(SubValue);
                        eos(&mut iter)?;

                        let q = Query::GetWhereBetween(spec, subval_lo, subval_hi);
                        return Ok(q);
                    }
                    _ => {
                        let optdat = opt_datum(&mut iter)?;
                        eos(&mut iter)?;

                        let subval = optdat.map(SubValue);
                        let q = Query::GetWhere(spec, subval);
                        return Ok(q);
                    }
                }
            }
            _ => {
                let dat = datum(&mut iter)?;
                let key = PrimaryKey(dat);
                eos(&mut iter)?;

                let q = Query::Get(key);
                return Ok(q);
            }
        },
        Some("create") => match iter.next() {
            Some("index") => {
                let spec = subvalspec(&mut iter)?;
                eos(&mut iter)?;
                return Ok(Query::CreateSecIdx(spec));
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
        Some("str") => return Ok(SubValueSpec::Whole(DatumType::Str)),
        Some("int") => return Ok(SubValueSpec::Whole(DatumType::I64)),
        Some("tup") => match iter.next() {
            Some("(") => match iter.next() {
                Some(int_literal) => {
                    let member_idx = int_literal.parse::<usize>().context(format!(
                        "Expected subvalspec tuple member_idx but found {:?}",
                        int_literal
                    ))?;
                    let member_spec = subvalspec(iter)?;
                    let member_spec = Box::new(member_spec);
                    match iter.next() {
                        Some(")") => {
                            return Ok(SubValueSpec::PartialTuple {
                                member_idx,
                                member_spec,
                            })
                        }
                        x => {
                            return Err(anyhow!(
                                "Expected closing of subvalspec tuple but found {:?}",
                                x
                            ))
                        }
                    }
                }
                x => return Err(anyhow!("Expected subvalspec tuple member {:?}", x)),
            },
            x => {
                return Err(anyhow!(
                    "Expected opening of subvalspec tuple but found {:?}",
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
        let exp_q_obj = Query::Put(
            PrimaryKey(Datum::I64(123)),
            Value(Datum::Str(String::from("val1"))),
        );
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        let q_str = "put tup( str(a) int(123) ) int(321)";
        let exp_q_obj = Query::Put(
            PrimaryKey(Datum::Tuple(vec![
                Datum::Str(String::from("a")),
                Datum::I64(123),
            ])),
            Value(Datum::I64(321)),
        );
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        Ok(())
    }

    #[test]
    fn del() -> Result<()> {
        let q_str = "del int(123)";
        let exp_q_obj = Query::Del(PrimaryKey(Datum::I64(123)));
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        Ok(())
    }

    #[test]
    fn get() -> Result<()> {
        let q_str = "get int(123)";
        let exp_q_obj = Query::Get(PrimaryKey(Datum::I64(123)));
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        let q_str = "get str(key1)";
        let exp_q_obj = Query::Get(PrimaryKey(Datum::Str(String::from("key1"))));
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        let q_str = "get tup( str(a) int(123) )";
        let exp_q_obj = Query::Get(PrimaryKey(Datum::Tuple(vec![
            Datum::Str(String::from("a")),
            Datum::I64(123),
        ])));
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        Ok(())
    }

    #[test]
    fn get_between() -> Result<()> {
        let q_str = "get between int(123) int(234)";
        let exp_q_obj = Query::GetBetween(
            Some(PrimaryKey(Datum::I64(123))),
            Some(PrimaryKey(Datum::I64(234))),
        );
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        let q_str = "get between int(123) _";
        let exp_q_obj = Query::GetBetween(Some(PrimaryKey(Datum::I64(123))), None);
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        let q_str = "get between _ int(234)";
        let exp_q_obj = Query::GetBetween(None, Some(PrimaryKey(Datum::I64(234))));
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        let q_str = "get between _ _";
        let exp_q_obj = Query::GetBetween(None, None);
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        Ok(())
    }

    #[test]
    fn get_where() -> Result<()> {
        let q_str = "get where int _";
        let exp_q_obj = Query::GetWhere(SubValueSpec::Whole(DatumType::I64), None);
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        let q_str = "get where int int(123)";
        let exp_q_obj = Query::GetWhere(
            SubValueSpec::Whole(DatumType::I64),
            Some(SubValue(Datum::I64(123))),
        );
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        let q_str = "get where tup( 1 tup( 0 str ) ) str(subval_a)";
        let exp_q_obj = Query::GetWhere(
            SubValueSpec::PartialTuple {
                member_idx: 1,
                member_spec: Box::new(SubValueSpec::PartialTuple {
                    member_idx: 0,
                    member_spec: Box::new(SubValueSpec::Whole(DatumType::Str)),
                }),
            },
            Some(SubValue(Datum::Str(String::from("subval_a")))),
        );
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        Ok(())
    }

    #[test]
    fn get_where_between() -> Result<()> {
        let q_str = "get where int between int(123) int(234)";
        let exp_q_obj = Query::GetWhereBetween(
            SubValueSpec::Whole(DatumType::I64),
            Some(SubValue(Datum::I64(123))),
            Some(SubValue(Datum::I64(234))),
        );
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        let q_str = "get where int between int(123) _";
        let exp_q_obj = Query::GetWhereBetween(
            SubValueSpec::Whole(DatumType::I64),
            Some(SubValue(Datum::I64(123))),
            None,
        );
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        let q_str = "get where int between _ int(234)";
        let exp_q_obj = Query::GetWhereBetween(
            SubValueSpec::Whole(DatumType::I64),
            None,
            Some(SubValue(Datum::I64(234))),
        );
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        let q_str = "get where int between _ _";
        let exp_q_obj = Query::GetWhereBetween(SubValueSpec::Whole(DatumType::I64), None, None);
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        Ok(())
    }

    #[test]
    fn create_secidx() -> Result<()> {
        let q_str = "create index int";
        let exp_q_obj = Query::CreateSecIdx(SubValueSpec::Whole(DatumType::I64));
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        let q_str = "create index tup( 2 int )";
        let exp_q_obj = Query::CreateSecIdx(SubValueSpec::PartialTuple {
            member_idx: 2,
            member_spec: Box::new(SubValueSpec::Whole(DatumType::I64)),
        });
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        let q_str = "create index tup( 1 tup( 0 str ) )";
        let exp_q_obj = Query::CreateSecIdx(SubValueSpec::PartialTuple {
            member_idx: 1,
            member_spec: Box::new(SubValueSpec::PartialTuple {
                member_idx: 0,
                member_spec: Box::new(SubValueSpec::Whole(DatumType::Str)),
            }),
        });
        let q_obj = parse(q_str)?;
        assert!(q_obj == exp_q_obj);

        Ok(())
    }
}
