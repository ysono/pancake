#[cfg(test)]
mod test {
    use super::super::*;
    use std::cmp::PartialEq;
    use std::collections::VecDeque;
    use std::fmt::Debug;
    use std::iter;

    fn assert_node_elem<T>(exp_elem: &T, node_ptr: NonNullSendPtr<ListNode<T>>)
    where
        T: Debug + PartialEq,
    {
        let node_ref = unsafe { node_ptr.as_ref() };
        assert_eq!(exp_elem, &node_ref.elem);
    }

    fn assert_list_elems<T>(exp_elems: &VecDeque<T>, lst: &AtomicLinkedList<T>)
    where
        T: Debug + PartialEq,
    {
        let exp = exp_elems.iter().collect::<Vec<_>>();
        let act = lst.snap().iter().collect::<Vec<_>>();
        assert_eq!(exp, act);
    }

    fn assert_snap_elems<T>(exp_elems: impl Iterator<Item = T>, snap: ListSnapshot<T>)
    where
        T: 'static + Debug + PartialEq + Clone,
    {
        let exp = exp_elems.collect::<Vec<_>>();
        let act = snap.iter().cloned().collect::<Vec<_>>();
        assert_eq!(exp, act);
    }

    fn push_then_assert<T>(
        elem: T,
        exp: &mut VecDeque<T>,
        lst: &mut AtomicLinkedList<T>,
    ) -> NonNullSendPtr<ListNode<T>>
    where
        T: Debug + PartialEq + Clone,
    {
        let node_ptr = lst.push_head_node_noncontested(ListNode::new(elem.clone()));
        assert_node_elem(&elem, node_ptr);

        exp.push_front(elem);
        assert_list_elems(&exp, &lst);

        node_ptr
    }

    #[test]
    fn serial() {
        let mut lst = AtomicLinkedList::<i32>::from_elems(iter::empty());

        let mut exp_elems = VecDeque::new();

        assert_list_elems(&exp_elems, &lst);

        for elem in 0..=4 {
            push_then_assert(elem, &mut exp_elems, &mut lst);
        }

        let node4_ptr = lst.head_node_ptr();
        let snap_4_0 = lst.snap();

        for elem in 5..=9 {
            push_then_assert(elem, &mut exp_elems, &mut lst);
        }

        let snap_9_0 = lst.snap();
        let snap_9_5 = ListSnapshot::new_unchecked(lst.head_node_ptr(), node4_ptr);

        for elem in 10..=14 {
            push_then_assert(elem, &mut exp_elems, &mut lst);
        }

        assert_snap_elems((0..=4).rev(), snap_4_0);
        assert_snap_elems((0..=9).rev(), snap_9_0);
        assert_snap_elems((5..=9).rev(), snap_9_5);
    }
}
