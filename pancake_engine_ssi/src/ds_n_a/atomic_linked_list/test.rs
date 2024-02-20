#[cfg(test)]
mod test {
    use super::super::*;
    use std::cmp::PartialEq;
    use std::collections::{HashSet, VecDeque};
    use std::fmt::Debug;
    use std::iter;
    use std::sync::Arc;
    use std::time::Duration;

    fn elem_into_node<T>(elem: T) -> Box<ListNode<T>> {
        Box::new(ListNode {
            elem,
            next: AtomicPtr::default(),
        })
    }

    fn new_snap_from_nonsend<T>(
        head_ptr: NonNull<ListNode<T>>,
        tail_ptr: Option<NonNull<ListNode<T>>>,
    ) -> ListSnapshot<T> {
        ListSnapshot::new(
            NonNullSendPtr::from(head_ptr),
            tail_ptr.map(NonNullSendPtr::from),
        )
    }

    fn assert_node_elem<T>(exp_elem: T, node_ptr: NonNull<ListNode<T>>)
    where
        T: Debug + PartialEq,
    {
        let node_ref = unsafe { node_ptr.as_ref() };
        assert_eq!(&exp_elem, &node_ref.elem);
    }

    fn assert_list_elems<T>(exp_elems: &VecDeque<T>, lst: &AtomicLinkedList<T>)
    where
        T: Debug + PartialEq,
    {
        let exp = exp_elems.iter().collect::<Vec<_>>();
        let act = lst.iter().collect::<Vec<_>>();
        assert_eq!(exp, act);
    }

    fn assert_snap_elems<T>(exp_elems: impl Iterator<Item = T>, snap: ListSnapshot<T>)
    where
        T: 'static + Debug + PartialEq + Clone,
    {
        let exp = exp_elems.collect::<Vec<_>>();
        let act = snap
            .into_iter_including_head_excluding_tail()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(exp, act);
    }

    #[test]
    fn serial() {
        let lst = AtomicLinkedList::<i32>::from_elems(iter::empty());

        let mut exp_elems = VecDeque::new();

        assert_list_elems(&exp_elems, &lst);

        let mut push_then_assert = |elem: i32| {
            let node_ptr = lst.push_head_node(elem_into_node(elem));
            assert_node_elem(elem, node_ptr);

            exp_elems.push_front(elem);
            assert_list_elems(&exp_elems, &lst);

            node_ptr
        };

        let mut head_ptr = NonNull::dangling();
        for elem in 0..=4 {
            head_ptr = push_then_assert(elem);
        }
        let node4_ptr = head_ptr;

        let iter_4_0 = new_snap_from_nonsend(head_ptr, None);

        for elem in 5..=9 {
            head_ptr = push_then_assert(elem);
        }

        let iter_9_0 = new_snap_from_nonsend(head_ptr, None);
        let iter_9_5 = new_snap_from_nonsend(head_ptr, Some(node4_ptr));

        for elem in 10..=14 {
            push_then_assert(elem);
        }

        assert_snap_elems((0..=4).rev(), iter_4_0);
        assert_snap_elems((0..=9).rev(), iter_9_0);
        assert_snap_elems((5..=9).rev(), iter_9_5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn push_concurrent() {
        let val_ceil = 500usize;

        let lst = AtomicLinkedList::<usize>::from_elems(iter::empty());
        let lst = Arc::new(lst);

        let mut tasks = vec![];
        for val in 0..val_ceil {
            let lst_cloned = Arc::clone(&lst);
            let task = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(1)).await;
                lst_cloned.push_head_node(elem_into_node(val));
            });
            tasks.push(task);
        }
        for task in tasks.into_iter() {
            task.await.unwrap();
        }

        let exp_vals = (0..val_ceil).collect::<HashSet<_>>();
        let actual_vals = lst.iter().map(|val| val.clone()).collect::<HashSet<_>>();
        assert_eq!(exp_vals, actual_vals,);
    }
}
