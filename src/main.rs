use pancake::storage::api::*;
use pancake::storage::lsm;
use rand;
use std::collections::BTreeSet;

fn put(s: &mut lsm::State, k: String, v: Option<String>) {
    s.put(Key(k), v.map(|v| Value::Bytes(v.as_bytes().to_vec())))
        .unwrap();
}

fn get(s: &mut lsm::State, k: String) -> Option<Value> {
    s.get(Key(k)).unwrap()
}

fn get_print(s: &mut lsm::State, k: String) {
    match get(s, k.clone()) {
        None => {
            println!("{} ... No such key", k)
        }
        Some(Value::Bytes(vec)) => {
            println!("{} ---> {}", k, String::from_utf8(vec).unwrap());
        }
    }
}

fn main() {
    let mut s = lsm::State::init().unwrap();

    let mut ii = BTreeSet::new();

    for _ in 0..37 {
        let i = rand::random::<u8>();

        let insert = rand::random::<f32>() < 0.7;

        let key = format!("key{}", i);
        let val = if insert {
            Some(format!("val{}", i))
        } else {
            None
        };

        put(&mut s, key, val);

        ii.insert(i);
    }

    for i in ii.iter() {
        get_print(&mut s, format!("key{}", i));
    }
}
