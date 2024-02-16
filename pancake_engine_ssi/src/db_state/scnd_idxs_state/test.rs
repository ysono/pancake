#[cfg(test)]
mod test {
    use super::super::*;
    use pancake_types::serde::DatumType;
    use rand::prelude::*;

    #[test]
    fn empty_serialized_bytes() {
        let content = "";
        let mut r = BufReader::new(Cursor::new(content.as_bytes()));
        let res = ScndIdxsState::do_deser(&mut r);
        match res {
            Err(e) => assert_eq!(e.to_string(), "Invalid next_scnd_idx_num"),
            Ok(_) => panic!("empty"),
        }
    }

    fn ser_then_deser(orig: ScndIdxsState) {
        let mut buf = vec![];

        orig.do_ser(&mut BufWriter::new(Cursor::new(&mut buf)))
            .unwrap();

        let deserialized = ScndIdxsState::do_deser(&mut BufReader::new(Cursor::new(&buf))).unwrap();

        assert_eq!(orig, deserialized);
    }

    fn gen_sv_spec_whole() -> Arc<SubValueSpec> {
        Arc::new(SubValueSpec {
            member_idxs: vec![],
            datum_type: DatumType::I64,
        })
    }
    fn gen_sv_spec_partial() -> Arc<SubValueSpec> {
        Arc::new(SubValueSpec {
            member_idxs: vec![6, 5, 4, 3],
            datum_type: DatumType::I64,
        })
    }

    fn gen_si_state_sample1() -> ScndIdxState {
        ScndIdxState {
            scnd_idx_num: ScndIdxNum(5),
            is_readable: false,
        }
    }
    fn gen_si_state_sample2() -> ScndIdxState {
        ScndIdxState {
            scnd_idx_num: ScndIdxNum(6),
            is_readable: true,
        }
    }

    #[test]
    fn ser_then_deser_arbitrary_examples() {
        /* setup */
        let gen_sv_fns = [gen_sv_spec_whole, gen_sv_spec_partial];
        let gen_si_state_fns = [gen_si_state_sample1, gen_si_state_sample2];
        let mut rand_rng = rand::thread_rng();

        /* empty */
        let sis_state = ScndIdxsState {
            scnd_idxs: HashMap::new(),
            next_scnd_idx_num: ScndIdxNum(123),
        };
        ser_then_deser(sis_state);

        /* one si */
        for gen_sv in &gen_sv_fns {
            for gen_si_state in &gen_si_state_fns {
                let mut scnd_idxs = HashMap::new();
                scnd_idxs.insert(gen_sv(), gen_si_state());
                let sis_state = ScndIdxsState {
                    scnd_idxs,
                    next_scnd_idx_num: ScndIdxNum(123),
                };
                ser_then_deser(sis_state);
            }
        }

        /* multiple sis */
        for _ in 0..5 {
            let mut scnd_idxs = HashMap::new();
            for _ in 1..(rand_rng.gen::<u8>() % 7) {
                let gen_sv = gen_sv_fns.choose(&mut rand_rng).unwrap();
                let gen_si_state = gen_si_state_fns.choose(&mut rand_rng).unwrap();
                scnd_idxs.insert(gen_sv(), gen_si_state());
            }
            let sis_state = ScndIdxsState {
                scnd_idxs,
                next_scnd_idx_num: ScndIdxNum(123),
            };
            ser_then_deser(sis_state);
        }
    }
}
