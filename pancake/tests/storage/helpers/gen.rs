use pancake::storage::serde::Datum;
use pancake::storage::types::{PrimaryKey, SubValue, Value};

pub fn gen_str_pk<S: AsRef<str>>(s: S) -> PrimaryKey {
    let dat = Datum::Str(String::from(s.as_ref()));
    PrimaryKey(dat)
}

pub fn gen_str_pv<S: AsRef<str>>(s: S) -> Value {
    let dat = Datum::Str(String::from(s.as_ref()));
    Value(dat)
}

pub fn gen_str_pkv(pk: &str, pv: &str) -> (PrimaryKey, Value) {
    (gen_str_pk(pk), gen_str_pv(pv))
}

pub fn gen_str_sv<S: AsRef<str>>(s: S) -> SubValue {
    let dat = Datum::Str(String::from(s.as_ref()));
    SubValue(dat)
}
