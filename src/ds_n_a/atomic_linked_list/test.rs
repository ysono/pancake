use super::*;
use std::cmp::PartialEq;
use std::collections::{HashSet, VecDeque};
use std::iter;
use std::sync::Arc;
use std::time::Duration;

impl<T> PartialEq for ListElem<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        match self {
            Self::Elem(self_val) => match other {
                Self::Elem(other_val) => return self_val == other_val,
                _ => return false,
            },
            Self::Dummy { is_terminus } => {
                let self_is_terminus = is_terminus.load(Ordering::Acquire);
                match other {
                    Self::Dummy { is_terminus } => {
                        let other_is_terminus = is_terminus.load(Ordering::Acquire);
                        return self_is_terminus == other_is_terminus;
                    }
                    _ => return false,
                }
            }
        }
    }
}

fn assert_list_content<T>(lst: &AtomicLinkedList<T>, exp_elems: &VecDeque<ListElem<T>>)
where
    T: Debug + PartialEq,
{
    let actual = lst.iter().collect::<Vec<_>>();
    let exp_elem_refs = exp_elems.iter().collect::<Vec<_>>();
    assert_eq!(actual, exp_elem_refs);
}

fn assert_node_content<T>(node: *const ListNode<T>, exp_elem: ListElem<T>)
where
    T: Debug + PartialEq,
{
    let node = unsafe { &*node };
    assert_eq!(&node.elem, &exp_elem);
}

#[test]
fn push_serial() {
    let mut exp_elems = VecDeque::new();

    let lst = AtomicLinkedList::<i32>::new(iter::empty());

    assert_list_content(&lst, &exp_elems);

    for val in 0..5 {
        let node = lst.push_newest(ListElem::Elem(val));
        assert_node_content(node, ListElem::Elem(val));

        exp_elems.push_front(ListElem::Elem(val));
        assert_list_content(&lst, &exp_elems);
    }

    {
        let node = lst.push_newest(ListElem::new_dummy(false));
        assert_node_content(node, ListElem::new_dummy(false));

        exp_elems.push_front(ListElem::new_dummy(false));
        assert_list_content(&lst, &exp_elems);
    }

    for val in 6..10 {
        let node = lst.push_newest(ListElem::Elem(val));
        assert_node_content(node, ListElem::Elem(val));

        exp_elems.push_front(ListElem::Elem(val));
        assert_list_content(&lst, &exp_elems);
    }
}

#[tokio::test]
async fn push_concurrent() {
    let val_ceil = 500usize;

    let lst = AtomicLinkedList::<usize>::new(iter::empty());
    let lst = Arc::new(lst);

    let mut tasks = vec![];
    for val in 0..val_ceil {
        let lst = Arc::clone(&lst);
        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            lst.push_newest(ListElem::Elem(val));
        });
        tasks.push(task);
    }
    for task in tasks.into_iter() {
        task.await.unwrap();
    }

    let actual_vals = lst
        .iter()
        .map(|elem| match elem {
            ListElem::Elem(val) => val.clone(),
            _ => panic!("Unexpected elem type."),
        })
        .collect::<HashSet<_>>();
    assert_eq!(actual_vals.len(), val_ceil);
}
