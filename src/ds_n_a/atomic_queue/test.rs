use super::*;

#[test]
fn serial() {
    fn push_then_pop<T>(qu: &AtomicQueue<T>, exp: Vec<T>)
    where
        T: Debug + PartialEq + Copy,
    {
        for val in exp.iter() {
            qu.push(val.clone());
            assert_eq!(qu.peek(), Some(&exp[0]));
        }

        for i in 0..exp.len() {
            assert_eq!(qu.peek(), Some(&exp[i]));
            assert_eq!(qu.pop(), Some(exp[i]));
        }

        assert_eq!(qu.peek(), None);
        assert_eq!(qu.pop(), None);
    }

    let qu = AtomicQueue::new();

    assert_eq!(qu.peek(), None);
    assert_eq!(qu.pop(), None);

    push_then_pop(&qu, vec![1]);
    push_then_pop(&qu, vec![1, 2]);
    push_then_pop(&qu, vec![1, 2, 3]);
    push_then_pop(&qu, vec![1, 2, 3, 4]);
}
