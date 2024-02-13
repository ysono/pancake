#[cfg(test)]
mod test {
    use super::super::*;

    impl<T> PartialEq for Interval<T>
    where
        T: Eq,
    {
        fn eq(&self, other: &Self) -> bool {
            self.lo_incl == other.lo_incl && self.hi_incl == other.hi_incl
        }
    }

    fn add_helper<T>(is: &mut IntervalSet<T>, lo_incl: Option<T>, hi_incl: Option<T>) {
        is.add(Interval { lo_incl, hi_incl });
    }

    fn assert_content<T>(is: &IntervalSet<T>, exp: Vec<(Option<T>, Option<T>)>)
    where
        T: Ord + Debug,
    {
        let act = is.iter().collect::<Vec<_>>();
        let exp = exp
            .into_iter()
            .map(|(lo_incl, hi_incl)| Interval { lo_incl, hi_incl })
            .collect::<Vec<_>>();
        let exp = exp.iter().collect::<Vec<_>>();
        assert_eq!(act, exp);
    }

    #[test]
    fn merging() {
        let mut is = IntervalSet::<i32>::new();
        assert_content(&is, vec![]);
        is.merge();
        assert_content(&is, vec![]);

        // Add first range
        add_helper(&mut is, Some(10), Some(20));
        is.merge();
        assert_content(&is, vec![(Some(10), Some(20))]);

        // Add disjoint
        add_helper(&mut is, Some(30), Some(40));
        is.merge();
        assert_content(&is, vec![(Some(10), Some(20)), (Some(30), Some(40))]);

        // Add disjoint
        add_helper(&mut is, Some(50), Some(60));
        is.merge();
        assert_content(
            &is,
            vec![
                (Some(10), Some(20)),
                (Some(30), Some(40)),
                (Some(50), Some(60)),
            ],
        );

        // Add subsumed
        add_helper(&mut is, Some(13), Some(17));
        is.merge();
        assert_content(
            &is,
            vec![
                (Some(10), Some(20)),
                (Some(30), Some(40)),
                (Some(50), Some(60)),
            ],
        );

        // Add disjoint, with gaps of one
        add_helper(&mut is, Some(21), Some(29));
        is.merge();
        assert_content(
            &is,
            vec![
                (Some(10), Some(20)),
                (Some(21), Some(29)),
                (Some(30), Some(40)),
                (Some(50), Some(60)),
            ],
        );

        // Expand one range, low-ward
        add_helper(&mut is, Some(47), Some(55));
        is.merge();
        assert_content(
            &is,
            vec![
                (Some(10), Some(20)),
                (Some(21), Some(29)),
                (Some(30), Some(40)),
                (Some(47), Some(60)),
            ],
        );

        // Expand one range, high-ward
        add_helper(&mut is, Some(35), Some(43));
        is.merge();
        assert_content(
            &is,
            vec![
                (Some(10), Some(20)),
                (Some(21), Some(29)),
                (Some(30), Some(43)),
                (Some(47), Some(60)),
            ],
        );

        // Combine two ranges
        add_helper(&mut is, Some(43), Some(47));
        is.merge();
        assert_content(
            &is,
            vec![
                (Some(10), Some(20)),
                (Some(21), Some(29)),
                (Some(30), Some(60)),
            ],
        );

        // Add a (-inf ... x]
        add_helper(&mut is, None, Some(10));
        is.merge();
        assert_content(
            &is,
            vec![(None, Some(20)), (Some(21), Some(29)), (Some(30), Some(60))],
        );

        // And a [x ... +inf)
        add_helper(&mut is, Some(29), None);
        is.merge();
        assert_content(&is, vec![(None, Some(20)), (Some(21), None)]);

        // Add a (-inf ... +inf)
        add_helper(&mut is, None, None);
        is.merge();
        assert_content(&is, vec![(None, None)]);

        // Add subsumed
        add_helper(&mut is, Some(10), Some(20));
        is.merge();
        assert_content(&is, vec![(None, None)]);
    }

    fn assert_overlapping<T>(is: &IntervalSet<T>, points: Vec<T>, exp: bool) -> Result<()>
    where
        T: Ord + Debug,
    {
        assert_eq!(exp, is.overlaps_with(points.into_iter())?,);
        Ok(())
    }

    #[test]
    fn overlapping() -> Result<()> {
        let mut is = IntervalSet::<i32>::new();

        assert_overlapping(&is, vec![1, 2, 3], false)?;

        add_helper(&mut is, Some(20), Some(30));
        is.merge();
        assert_overlapping(&is, vec![15, 35], false)?;
        assert_overlapping(&is, vec![25], true)?;
        assert_overlapping(&is, vec![15, 30, 35], true)?;

        add_helper(&mut is, None, Some(10));
        is.merge();
        assert_overlapping(&is, vec![15, 35], false)?;
        assert_overlapping(&is, vec![-999], true)?;
        assert_overlapping(&is, vec![10], true)?;
        assert_overlapping(&is, vec![20], true)?;
        assert_overlapping(&is, vec![5, 15, 25, 35], true)?;

        add_helper(&mut is, Some(40), None);
        is.merge();
        assert_overlapping(&is, vec![15, 35], false)?;
        assert_overlapping(&is, vec![-999], true)?;
        assert_overlapping(&is, vec![10], true)?;
        assert_overlapping(&is, vec![20], true)?;
        assert_overlapping(&is, vec![40], true)?;
        assert_overlapping(&is, vec![5, 15, 25, 35, 45], true)?;

        Ok(())
    }
}
