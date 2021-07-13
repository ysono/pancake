use pancake::storage;
use pancake::storage::api::*;

fn main() {
    let mut lsm_state = storage::lsm::State::init().unwrap();
    
    storage::lsm::put(
        &mut lsm_state,
        Key(String::from("myint")),
        Some(Value::Integer(3)));

    storage::lsm::put(
        &mut lsm_state,
        Key(String::from("mytext")),
        Some(Value::Text(String::from("aloha"))));
    
    let gotten = storage::lsm::get(
        &lsm_state,
        Key(String::from("myint"))
    );
    match gotten {
        Some(v) => { println!("{:?}", v); },
        None => { println!("No such key"); },
    }

    let gotten = storage::lsm::get(
        &lsm_state,
        Key(String::from("mytext"))
    );
    match gotten {
        Some(v) => { println!("{:?}", v); },
        None => { println!("No such key"); },
    }
}
