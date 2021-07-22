use pancake::storage::api::*;
use pancake::storage::lsm;
use rand;
use std::collections::BTreeMap;

fn put(lsm: &mut lsm::LSM, k: &String, v: Option<String>) {
    let v = match v {
        None => Value::Tombstone,
        Some(v) => Value::Bytes(v.as_bytes().to_vec()),
    };
    lsm.put(Key(k.clone()), v).unwrap();
}

fn get(lsm: &mut lsm::LSM, k: String) -> Option<Value> {
    lsm.get(Key(k)).unwrap()
}

fn test_get(lsm: &mut lsm::LSM, k: String, exp_deleted: bool) {
    match get(lsm, k.clone()) {
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
    let mut lsm = lsm::LSM::init().unwrap();

    let mut i_to_deleted = BTreeMap::new();

    for _ in 0..600 {
        let i = rand::random::<u8>();

        let key = format!("key{}", i);
        let val = Some(format!("val{}", i));

        put(&mut lsm, &key, val);

        let is_deleted = rand::random::<f32>() < 0.3;
        if is_deleted {
            put(&mut lsm, &key, None);
            println!("key {} is del.", i);
        }
        i_to_deleted.insert(i, is_deleted);
    }

    for (i, is_deleted) in i_to_deleted.iter() {
        test_get(&mut lsm, format!("key{}", i), *is_deleted);
    }

    test_get(&mut lsm, String::from("nonexistent"), true);
}
