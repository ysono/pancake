use crate::ds_n_a::cmp::TryPartialOrd;
use anyhow::{anyhow, Result};
use std::cmp::Ordering;
use std::fmt::Debug;
use std::mem;

#[derive(Debug)]
pub struct Interval<T> {
    pub lo_incl: Option<T>,
    pub hi_incl: Option<T>,
}

#[derive(Debug)]
pub struct IntervalSet<T> {
    itvs: Vec<Interval<T>>,
    is_merged: bool,
}

impl<T> IntervalSet<T> {
    pub fn new() -> Self {
        Self {
            itvs: Vec::default(),
            is_merged: true,
        }
    }

    pub fn add(&mut self, itv: Interval<T>) {
        self.itvs.push(itv);
        self.is_merged = false;
    }

    pub fn iter(&self) -> impl Iterator<Item = &Interval<T>> {
        self.itvs.iter()
    }

    pub fn clear(&mut self) {
        self.itvs.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.itvs.is_empty()
    }
}

impl<T> IntervalSet<T>
where
    T: Ord,
{
    pub fn merge(&mut self) {
        if self.itvs.is_empty() || self.is_merged {
            return;
        }

        let mut old_itvs = mem::replace(&mut self.itvs, vec![]);
        old_itvs.sort_by(|a, b| match (a.lo_incl.as_ref(), b.lo_incl.as_ref()) {
            (None, _) => Ordering::Less,
            (_, None) => Ordering::Greater,
            (Some(a_lo), Some(b_lo)) => a_lo.cmp(b_lo),
        });

        let mut old_itvs = old_itvs.into_iter();

        let mut prev_itv = old_itvs.next().unwrap();

        for curr_itv in old_itvs {
            match (&prev_itv.hi_incl, &curr_itv.lo_incl) {
                (None, _) => break,
                (Some(prev_hi), opt_curr_lo) => {
                    let is_overlapping = match opt_curr_lo {
                        None => true,
                        Some(curr_lo) => curr_lo <= prev_hi,
                    };
                    if is_overlapping {
                        let curr_hi_is_greater = match curr_itv.hi_incl.as_ref() {
                            None => true,
                            Some(curr_hi) => prev_hi < curr_hi,
                        };
                        if curr_hi_is_greater {
                            prev_itv.hi_incl = curr_itv.hi_incl;
                        }
                    } else {
                        let prev = mem::replace(&mut prev_itv, curr_itv);
                        self.itvs.push(prev);
                    }
                }
            }
        }

        self.itvs.push(prev_itv);

        self.is_merged = true;
    }
}

impl<T> IntervalSet<T> {
    pub fn overlaps_with<'a, U>(&'a self, point_iter: impl Iterator<Item = U>) -> Result<bool>
    where
        U: TryPartialOrd<T>,
    {
        if !self.is_merged {
            // This method does not call self.merge(), b/c merge() takes &mut self,
            // and we want overlaps_with() to take &self.
            return Err(anyhow!("Client must call merge() beforehand."));
        }

        let mut itv_iter = self.itvs.iter().peekable();
        let mut point_iter = point_iter.peekable();

        loop {
            match (itv_iter.peek(), point_iter.peek()) {
                (None, _) => return Ok(false),
                (_, None) => return Ok(false),
                (Some(Interval { lo_incl, hi_incl }), Some(point)) => {
                    /* Check whether curr point is less than curr itv. */
                    if let Some(lo_incl) = lo_incl.as_ref() {
                        match point.try_partial_cmp(lo_incl)? {
                            Some(Ordering::Less) => {
                                point_iter.next();
                                continue;
                            }
                            Some(Ordering::Equal) | None => return Ok(true),
                            Some(Ordering::Greater) => (),
                        }
                    }
                    /* Check whether curr point is greater than curr itv. */
                    if let Some(hi_incl) = hi_incl.as_ref() {
                        match point.try_partial_cmp(hi_incl)? {
                            Some(Ordering::Greater) => {
                                itv_iter.next();
                                continue;
                            }
                            _ => (),
                        }
                    }
                    /* Curr point is contained wihin curr itv. */
                    return Ok(true);
                }
            }
        }
    }
}

#[cfg(test)]
mod test;
