use pancake::storage::api::*;
use pancake::storage::lsm;

fn put(s: &mut lsm::State, k: String, v: Option<String>) {
    lsm::put(s, Key(k), v.map(|v| Value::Bytes(v.as_bytes().to_vec()))).unwrap();
}

fn get(s: &mut lsm::State, k: String) -> Option<Value> {
    lsm::get(s, Key(k)).unwrap()
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

    for i in 0..37 {
        let v = if i % 5 == 0 {
            None
        } else {
            Some(format!("val{}", i))
        };
        put(&mut s, format!("key{}", i), v);
    }

    for i in 0..37 {
        get_print(&mut s, format!("key{}", i));
    }
}
