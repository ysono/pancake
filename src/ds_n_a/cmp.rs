use anyhow::Result;
use std::cmp::Ordering;

pub trait TryPartialOrd<Rhs> {
    fn try_partial_cmp(&self, other: &Rhs) -> Result<Option<Ordering>>;
}

impl<T> TryPartialOrd<T> for T
where
    T: Ord,
{
    fn try_partial_cmp(&self, other: &T) -> Result<Option<Ordering>> {
        Ok(Some(self.cmp(other)))
    }
}
