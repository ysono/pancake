use std::cmp::Ordering;
use std::convert::Infallible;

pub trait TryPartialOrd<Rhs, E> {
    fn try_partial_cmp(&self, rhs: &Rhs) -> Result<Option<Ordering>, E>;
}

impl<Lhs, Rhs> TryPartialOrd<Rhs, Infallible> for Lhs
where
    Lhs: PartialOrd<Rhs>,
{
    fn try_partial_cmp(&self, rhs: &Rhs) -> Result<Option<Ordering>, Infallible> {
        Ok(self.partial_cmp(rhs))
    }
}
