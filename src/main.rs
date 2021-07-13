use pancake::storage;
use pancake::storage::api::*;

fn main() {
    let mut lsm_state = storage::lsm::State::init().unwrap();
    
    storage::lsm::put(
        &mut lsm_state,
        Key(String::from("mykey")),
        Some(Value::Bytes("myvalue".as_bytes().to_vec())));

    storage::lsm::put(
        &mut lsm_state,
        Key(String::from("mykey2")),
        Some(Value::Bytes("myvalue2".as_bytes().to_vec())));
    
    storage::lsm::put(
        &mut lsm_state,
        Key(String::from("mykey3")),
        Some(Value::Bytes("myvalue3".as_bytes().to_vec())));

    // storage::lsm::put(
    //     &mut lsm_state,
    //     Key(String::from("myint")),
    //     Some(Value::Integer(3)));

    // storage::lsm::put(
    //     &mut lsm_state,
    //     Key(String::from("mytext")),
    //     Some(Value::Text(String::from("aloha"))));

    let gotten = storage::lsm::get(
        &lsm_state,
        Key(String::from("mykey"))
    );
    match gotten {
        Some(Value::Bytes(v)) => {
            println!("{:?}", String::from_utf8(v).unwrap());
        },
        _ => { println!("No such key"); },
    }
    
    let gotten = storage::lsm::get(
        &lsm_state,
        Key(String::from("mykey2"))
    );
    match gotten {
        Some(Value::Bytes(v)) => { println!("{:?}", String::from_utf8(v).unwrap()); },
        None => { println!("No such key"); },
    }

    let gotten = storage::lsm::get(
        &lsm_state,
        Key(String::from("mykey3"))
    );
    match gotten {
        Some(Value::Bytes(v)) => { println!("{:?}", String::from_utf8(v).unwrap()); },
        None => { println!("No such key"); },
    }
}
