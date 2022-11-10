#[cfg(test)]
mod test {
    use crate::serde::{Datum, OptDatum, ReadResult};
    use anyhow::{anyhow, Result};
    use itertools::Itertools;
    use rand::seq::SliceRandom;
    use std::io::Cursor;

    fn verify(pre_serialized: &Vec<OptDatum<Datum>>) -> Result<()> {
        let (serialized, w_len_at_each_dat) = {
            let mut serialized: Vec<u8> = vec![];
            let mut w_len_at_each_dat: Vec<usize> = vec![]; // Cumulative `w_len`s.

            let w = &mut serialized;
            let mut w_len = 0;
            for optdat in pre_serialized {
                let delta_w_len = optdat.ser(w)?;
                w_len += *delta_w_len;
                w_len_at_each_dat.push(w_len);
            }
            assert_eq!(
                serialized.len(),
                w_len,
                "\n{:?}\n{:?}\n",
                pre_serialized,
                serialized
            );

            (serialized, w_len_at_each_dat)
        };

        {
            let mut r = Cursor::new(&serialized);
            let mut r_len = 0;
            for dat_i in 0..pre_serialized.len() {
                match OptDatum::<Datum>::skip(&mut r)? {
                    ReadResult::EOF => return Err(anyhow!("Premature EOF")),
                    ReadResult::Some(delta_r_len, ()) => r_len += delta_r_len,
                }
                assert_eq!(w_len_at_each_dat[dat_i], r_len);
            }
            assert_eq!(
                ReadResult::EOF,
                OptDatum::<Datum>::deser(&mut r)?,
                "\n{:?}\n{:?}\n",
                pre_serialized,
                serialized
            );
        }

        {
            let mut r = Cursor::new(&serialized);
            let mut r_len = 0;
            let mut deserialized: Vec<OptDatum<Datum>> = vec![];
            for dat_i in 0..pre_serialized.len() {
                match OptDatum::<Datum>::deser(&mut r)? {
                    ReadResult::EOF => return Err(anyhow!("Premature EOF")),
                    ReadResult::Some(delta_r_len, optdat) => {
                        r_len += delta_r_len;
                        deserialized.push(optdat);
                    }
                }
                assert_eq!(w_len_at_each_dat[dat_i], r_len);
            }
            assert_eq!(
                ReadResult::EOF,
                OptDatum::<Datum>::deser(&mut r)?,
                "\n{:?}\n{:?}\n",
                pre_serialized,
                serialized
            );
            assert_eq!(
                pre_serialized, &deserialized,
                "\n{:?}\n{:?}\n",
                pre_serialized, serialized
            );
        }

        Ok(())
    }

    fn gen_tomb() -> OptDatum<Datum> {
        OptDatum::Tombstone
    }
    fn gen_i64() -> OptDatum<Datum> {
        OptDatum::Some(Datum::I64(123))
    }
    fn gen_bytes() -> OptDatum<Datum> {
        OptDatum::Some(Datum::Bytes(String::from("asdf").into_bytes()))
    }
    fn gen_str() -> OptDatum<Datum> {
        OptDatum::Some(Datum::Str(String::from("asdf")))
    }
    fn gen_tup_depth1_memb1() -> OptDatum<Datum> {
        OptDatum::Some(Datum::Tuple(vec![Datum::Str(String::from("asdf"))]))
    }
    fn gen_tup_depth1_membmult() -> OptDatum<Datum> {
        OptDatum::Some(Datum::Tuple(vec![
            Datum::Str(String::from("asdf")),
            Datum::I64(9),
            Datum::Str(String::from("zxcv")),
        ]))
    }
    fn gen_tup_depth3() -> OptDatum<Datum> {
        OptDatum::Some(Datum::Tuple(vec![
            Datum::Str(String::from("asdf")),
            Datum::Tuple(vec![Datum::I64(456)]),
            Datum::I64(123),
        ]))
    }

    #[test]
    fn ser_then_deser() -> Result<()> {
        let mut rand_rng = rand::thread_rng();

        let gen_fns = [
            gen_tomb,
            gen_i64,
            gen_bytes,
            gen_str,
            gen_tup_depth1_memb1,
            gen_tup_depth1_membmult,
            gen_tup_depth3,
        ];

        for mut gen_fns in gen_fns.iter().powerset() {
            let datums = gen_fns.iter().map(|gen| gen()).collect::<Vec<_>>();
            verify(&datums)?;

            gen_fns.shuffle(&mut rand_rng);
            let datums = gen_fns.iter().map(|gen| gen()).collect::<Vec<_>>();
            verify(&datums)?;
        }

        Ok(())
    }
}
