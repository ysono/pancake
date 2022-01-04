use super::*;

fn gen_spec_whole() -> SubValueSpec {
    SubValueSpec::from(DatumType::Str)
}
fn gen_spec_partial_depth1() -> SubValueSpec {
    SubValueSpec {
        member_idxs: vec![1],
        datum_type: DatumType::Str,
    }
}
fn gen_spec_partial_depth2() -> SubValueSpec {
    SubValueSpec {
        member_idxs: vec![2, 1],
        datum_type: DatumType::Str,
    }
}

mod test_serde {
    use super::*;
    use std::io::Cursor;

    fn verify_serde(spec: SubValueSpec) -> Result<()> {
        let mut buf = vec![];
        spec.ser(&mut BufWriter::new(&mut buf))?;
        let deserialized = SubValueSpec::deser(&mut BufReader::new(Cursor::new(buf)))?;
        assert_eq!(spec, deserialized);
        Ok(())
    }

    #[test]
    fn test_serde() -> Result<()> {
        verify_serde(gen_spec_whole())?;
        verify_serde(gen_spec_partial_depth1())?;
        verify_serde(gen_spec_partial_depth2())?;
        Ok(())
    }
}

mod test_extract {
    use super::*;
    use crate::storage::types::{PVShared, SubValue};

    fn verify_extract(spec: SubValueSpec, pv: PVShared, exp_sv: Option<SubValue>) {
        let pv = Arc::new(pv);
        let act_sv = spec.extract(&pv);
        match (exp_sv, act_sv) {
            (None, None) => (),
            (Some(exp_sv), Some(act_sv)) => {
                assert_eq!(&exp_sv, &act_sv as &SubValue);
            }
            etc => panic!("{:?}", etc),
        }
    }

    #[test]
    fn test_extract() {
        verify_extract(
            gen_spec_whole(),
            PVShared::from(Datum::Str(String::from("asdf"))),
            Some(SubValue(Datum::Str(String::from("asdf")))),
        );
        verify_extract(gen_spec_whole(), PVShared::from(Datum::I64(123)), None);

        verify_extract(
            gen_spec_partial_depth1(),
            PVShared::from(Datum::Tuple(vec![
                Datum::I64(123),
                Datum::Str(String::from("asdf")),
                Datum::I64(123),
            ])),
            Some(SubValue(Datum::Str(String::from("asdf")))),
        );
        verify_extract(
            gen_spec_partial_depth1(),
            PVShared::from(Datum::Tuple(vec![
                Datum::I64(123),
                Datum::I64(123), // Not string.
                Datum::I64(123),
            ])),
            None,
        );
        verify_extract(
            gen_spec_partial_depth1(),
            PVShared::from(Datum::Tuple(vec![
                Datum::I64(123),
                // Missing member_idx 1.
            ])),
            None,
        );
        verify_extract(
            gen_spec_partial_depth1(),
            PVShared::from(Datum::Str(String::from("asdf"))), // Insufficient depth.
            None,
        );

        verify_extract(
            gen_spec_partial_depth2(),
            PVShared::from(Datum::Tuple(vec![
                Datum::I64(123),
                Datum::Str(String::from("asdf")),
                Datum::Tuple(vec![Datum::I64(456), Datum::Str(String::from("asdf2"))]),
            ])),
            Some(SubValue(Datum::Str(String::from("asdf2")))),
        );
        verify_extract(
            gen_spec_partial_depth2(),
            PVShared::from(Datum::Tuple(vec![
                Datum::I64(123),
                Datum::Str(String::from("asdf")),
                Datum::Tuple(vec![
                    Datum::I64(456),
                    Datum::I64(456), // Not string.
                ]),
            ])),
            None,
        );
        verify_extract(
            gen_spec_partial_depth2(),
            PVShared::from(Datum::Tuple(vec![
                Datum::I64(123),
                Datum::Str(String::from("asdf")),
                Datum::Tuple(vec![
                    Datum::I64(456),
                    // Missing member_idx 1.
                ]),
            ])),
            None,
        );
        verify_extract(
            gen_spec_partial_depth2(),
            PVShared::from(Datum::Tuple(vec![
                Datum::I64(123),
                Datum::Str(String::from("asdf")),
                Datum::I64(123), // Insufficient depth.
            ])),
            None,
        );
        verify_extract(
            gen_spec_partial_depth2(),
            PVShared::from(Datum::Str(String::from("asdf"))), // Insufficient depth.
            None,
        );
    }
}
