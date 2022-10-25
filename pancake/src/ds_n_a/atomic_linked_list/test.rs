use super::*;
use std::cmp::PartialEq;
use std::collections::{HashSet, VecDeque};
use std::fmt::Debug;
use std::iter;
use std::sync::Arc;
use std::time::Duration;

fn assert_node_elem<T>(node: *const ListNode<T>, exp_elem: T)
where
    T: Debug + PartialEq,
{
    let node = unsafe { &*node };
    assert_eq!(&node.elem, &exp_elem);
}

fn assert_list_elems<T>(lst: &AtomicLinkedList<T>, exp_elems: &VecDeque<T>)
where
    T: Debug + PartialEq,
{
    let actual = lst.iter().collect::<Vec<_>>();
    let exp_elem_refs = exp_elems.iter().collect::<Vec<_>>();
    assert_eq!(actual, exp_elem_refs);
}

fn assert_snap_elems<T>(snap: &AtomicLinkedListSnapshot<T>, exp_elems: Vec<T>)
where
    T: 'static + Debug + PartialEq,
{
    let actual = snap.iter().collect::<Vec<_>>();
    let expected = exp_elems.iter().collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[test]
fn serial() {
    let lst = AtomicLinkedList::<i32>::from_elems(iter::empty());

    let mut exp_elems = VecDeque::new();

    assert_list_elems(&lst, &exp_elems);

    let mut push_then_assert = |elem: i32| {
        let node = lst.push_elem(elem);
        assert_node_elem(node, elem);

        exp_elems.push_front(elem);
        assert_list_elems(&lst, &exp_elems);
    };

    for elem in 0..5 {
        push_then_assert(elem);
    }

    let head = lst.head().unwrap();
    let snap_3_0 = AtomicLinkedListSnapshot::<i32> {
        head_excl_ptr: SendPtr::from(head),
        tail_excl_ptr: None,
    };

    for elem in 5..10 {
        push_then_assert(elem);
    }

    let head = lst.head().unwrap();
    let snap_8_0 = AtomicLinkedListSnapshot::<i32> {
        head_excl_ptr: SendPtr::from(head),
        tail_excl_ptr: None,
    };
    let snap_8_5 = AtomicLinkedListSnapshot::<i32> {
        head_excl_ptr: SendPtr::from(head),
        tail_excl_ptr: Some(snap_3_0.head_excl_ptr),
    };

    for elem in 10..15 {
        push_then_assert(elem);
    }

    assert_snap_elems(&snap_3_0, (0..=3).rev().collect::<Vec<_>>());
    assert_snap_elems(&snap_8_0, (0..=8).rev().collect::<Vec<_>>());
    assert_snap_elems(&snap_8_5, (5..=8).rev().collect::<Vec<_>>());
}

#[tokio::test]
async fn push_concurrent() {
    let val_ceil = 500usize;

    let lst = AtomicLinkedList::<usize>::from_elems(iter::empty());
    let lst = Arc::new(lst);

    let mut tasks = vec![];
    let mut exp_vals = HashSet::new();
    for val in 0..val_ceil {
        let lst = Arc::clone(&lst);
        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            lst.push_elem(val);
        });
        tasks.push(task);
        exp_vals.insert(val);
    }
    for task in tasks.into_iter() {
        task.await.unwrap();
    }

    let actual_vals = lst.iter().map(|val| val.clone()).collect::<HashSet<_>>();
    assert_eq!(actual_vals, exp_vals);
}
