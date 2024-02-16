use anyhow::Result;
use pancake_engine_common::ds_n_a::cmp::TryPartialOrd;
use std::cmp::Ordering;
use std::fmt::Debug;

mod test;

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

    pub fn clear(&mut self) {
        self.itvs.clear();
        self.is_merged = true;
    }
}

impl<T> IntervalSet<T>
where
    T: Ord,
{
    pub fn merge(&mut self) -> MergedIntervalSet<T> {
        if !self.is_merged {
            self.itvs
                .sort_by(|a, b| match (a.lo_incl.as_ref(), b.lo_incl.as_ref()) {
                    (None, _) => Ordering::Less,
                    (_, None) => Ordering::Greater,
                    (Some(a_lo), Some(b_lo)) => a_lo.cmp(b_lo),
                });

            let mut i = 0;
            for j in 1..self.itvs.len() {
                match (&self.itvs[i].hi_incl, &self.itvs[j].lo_incl) {
                    (None, _) => break,
                    (Some(prev_hi), opt_curr_lo) => {
                        let is_overlapping = match opt_curr_lo {
                            None => true,
                            Some(curr_lo) => curr_lo <= prev_hi,
                        };
                        if is_overlapping {
                            let curr_hi_is_greater = match &self.itvs[j].hi_incl {
                                None => true,
                                Some(curr_hi) => prev_hi < curr_hi,
                            };
                            if curr_hi_is_greater {
                                self.itvs[i].hi_incl = self.itvs[j].hi_incl.take();
                            }
                        } else {
                            i += 1;
                            self.itvs[i].lo_incl = self.itvs[j].lo_incl.take();
                            self.itvs[i].hi_incl = self.itvs[j].hi_incl.take();
                        }
                    }
                }
            }

            self.itvs.truncate(i + 1);

            self.is_merged = true;
        }

        MergedIntervalSet { itvset: self }
    }
}

pub struct MergedIntervalSet<'a, T> {
    itvset: &'a IntervalSet<T>,
}

impl<'a, T> MergedIntervalSet<'a, T> {
    pub fn overlaps_with<P, E>(&self, point_iter: impl Iterator<Item = P>) -> Result<bool, E>
    where
        P: TryPartialOrd<T, E>,
    {
        let mut itv_iter = self.itvset.itvs.iter().peekable();
        let mut point_iter = point_iter.peekable();

        'walk: loop {
            match (itv_iter.peek(), point_iter.peek()) {
                (None, _) | (_, None) => return Ok(false),
                (Some(Interval { lo_incl, hi_incl }), Some(point)) => {
                    /* Compare point vs lo_incl. */
                    if let Some(lo_incl) = lo_incl.as_ref() {
                        match point.try_partial_cmp(lo_incl)? {
                            Some(Ordering::Less) => {
                                point_iter.next();
                                continue 'walk;
                            }
                            Some(Ordering::Equal) | None => return Ok(true),
                            Some(Ordering::Greater) => (),
                        }
                    }
                    /* lo_incl < point */
                    /* Compare point vs hi_incl. */
                    if let Some(hi_incl) = hi_incl.as_ref() {
                        match point.try_partial_cmp(hi_incl)? {
                            Some(Ordering::Greater) => {
                                itv_iter.next();
                                continue 'walk;
                            }
                            _ => (),
                        }
                    }
                    /* lo_incl < point <= hi_incl */
                    return Ok(true);
                }
            }
        }
    }
}
