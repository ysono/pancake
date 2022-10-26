use pancake_types::types::{PrimaryKey, SubValue, SubValueSpec, Value};

#[derive(PartialEq, Eq, Debug)]
pub enum Statement {
    GetPK(SearchRange<PrimaryKey>),
    GetSV(SubValueSpec, SearchRange<SubValue>),
    Put(PrimaryKey, Option<Value>),
}

#[allow(dead_code)] // `DelScndIdx` is never used. TODO support it in query language.
#[derive(PartialEq, Eq, Debug)]
pub enum Operation {
    Query(Statement),
    CreateScndIdx(SubValueSpec),
    DelScndIdx(SubValueSpec),
}

impl From<Statement> for Operation {
    fn from(stmt: Statement) -> Self {
        Self::Query(stmt)
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum SearchRange<T> {
    One(T),
    Range { lo: Option<T>, hi: Option<T> },
}

impl<T> SearchRange<T> {
    pub fn all() -> Self {
        Self::Range { lo: None, hi: None }
    }

    pub fn as_ref(&self) -> (Option<&T>, Option<&T>) {
        match &self {
            Self::One(one) => (Some(one), Some(one)),
            Self::Range { lo, hi } => (lo.as_ref(), hi.as_ref()),
        }
    }
}
