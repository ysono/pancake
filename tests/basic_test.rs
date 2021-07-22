use pancake::storage::api::*;
use pancake::storage::lsm;
use rand;
use std::collections::BTreeMap;

fn put(s: &mut lsm::LSM, k: &String, v: Option<String>) {
    let v = match v {
        None => Value::Tombstone,
        Some(v) => Value::Bytes(v.as_bytes().to_vec()),
    };
    s.put(Key(k.clone()), v).unwrap();
}

fn get(s: &mut lsm::LSM, k: String) -> Option<Value> {
    s.get(Key(k)).unwrap()
}

fn get_print(s: &mut lsm::LSM, k: String, exp_deleted: bool) {
    match get(s, k.clone()) {
        None | Some(Value::Tombstone) => {
            println!("{} ... No such key", k);
            assert!(exp_deleted);
        }
        Some(Value::Bytes(vec)) => {
            println!("{} ---> {}", k, String::from_utf8(vec).unwrap());
            assert!(!exp_deleted);
        }
    }
}

#[test]
fn random_data() {
    let mut s = lsm::LSM::init().unwrap();

    let mut i_to_deleted = BTreeMap::new();

    for _ in 0..37 {
        let i = rand::random::<u8>();

        let key = format!("key{}", i);
        let val = Some(format!("val{}", i));

        put(&mut s, &key, val);

        let is_deleted = rand::random::<f32>() < 0.3;
        if is_deleted {
            put(&mut s, &key, None);
            println!("key {} is del.", i);
        }
        i_to_deleted.insert(i, is_deleted);
    }

    for (i, is_deleted) in i_to_deleted.iter() {
        get_print(&mut s, format!("key{}", i), *is_deleted);
    }
}
