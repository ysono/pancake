use pancake::storage::lsm;
use pancake::storage::api::*;

fn put(s: &mut lsm::State, k: &str, v: Option<&str>) {
    lsm::put(
        s,
        Key(String::from(k)),
        v.map(|v| Value::Bytes(v.as_bytes().to_vec()))
    ).unwrap();
}

fn get(s: &mut lsm::State, k: &str) -> Option<Value> {
    lsm::get(
        s,
        Key(String::from(k))
    ).unwrap()
}
fn get_print(s: &mut lsm::State, k: &str) {
    match get(s, k) {
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

    put(&mut s, "key1", Some("val1"));
    put(&mut s, "key2", Some("valasdf2"));
    put(&mut s, "keyasdf3", None);
    put(&mut s, "key4", Some("v4"));
    
    get_print(&mut s, "key1");
    get_print(&mut s, "key2");
    get_print(&mut s, "keyasdf3");
    get_print(&mut s, "key4");
}
