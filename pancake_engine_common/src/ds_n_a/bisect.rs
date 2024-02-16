use std::cmp::Ordering;
use std::ops::Index;

pub fn bisect_left<Collxn, Elm, Cmp>(
    collxn: &Collxn,
    idx_lo_incl: usize,
    idx_hi_excl: usize,
    cmp: Cmp,
) -> usize
where
    Collxn: Index<usize, Output = Elm>,
    Cmp: Fn(&Elm) -> Ordering,
{
    let mut lo = idx_lo_incl;
    let mut hi = idx_hi_excl;
    while lo < hi {
        let md = lo + (hi - lo) / 2;
        let elm = &collxn[md];
        if cmp(elm).is_ge() {
            hi = md;
        } else {
            lo = md + 1;
        }
    }
    lo
}

pub fn bisect_right<Collxn, Elm, Cmp>(
    collxn: &Collxn,
    idx_lo_incl: usize,
    idx_hi_excl: usize,
    cmp: Cmp,
) -> usize
where
    Collxn: Index<usize, Output = Elm>,
    Cmp: Fn(&Elm) -> Ordering,
{
    let mut lo = idx_lo_incl;
    let mut hi = idx_hi_excl;
    while lo < hi {
        let md = lo + (hi - lo) / 2;
        let elm = &collxn[md];
        if cmp(elm).is_le() {
            lo = md + 1;
        } else {
            hi = md;
        }
    }
    lo
}

#[cfg(test)]
mod test {
    use super::*;

    fn check(vec: &Vec<i32>, search: i32, exp_left_idx: usize, exp_right_idx: usize) {
        let act_left_idx = bisect_left(vec, 0, vec.len(), |x| x.cmp(&search));
        if act_left_idx != exp_left_idx {
            panic!("L {vec:?} {search} {exp_left_idx} {act_left_idx}");
        }

        let act_right_idx = bisect_right(vec, 0, vec.len(), |x| x.cmp(&search));
        if act_right_idx != exp_right_idx {
            panic!("R {vec:?} {search} {exp_right_idx} {act_right_idx}");
        }
    }

    fn build_and_check([ct3, ct5, ct7]: [usize; 3]) {
        let mut vec = vec![];
        for _ in 0..ct3 {
            vec.push(3);
        }
        for _ in 0..ct5 {
            vec.push(5);
        }
        for _ in 0..ct7 {
            vec.push(7);
        }

        let ct_3_5 = ct3 + ct5;

        check(&vec, 2, 0, 0);
        check(&vec, 3, 0, ct3);
        check(&vec, 4, ct3, ct3);
        check(&vec, 5, ct3, ct_3_5);
        check(&vec, 6, ct_3_5, ct_3_5);
        check(&vec, 7, ct_3_5, vec.len());
        check(&vec, 8, vec.len(), vec.len());
    }

    #[test]
    fn rand() {
        for ct1 in 0..5 {
            for ct2 in 0..5 {
                for ct3 in 0..5 {
                    build_and_check([ct1, ct2, ct3]);
                }
            }
        }
    }
}
