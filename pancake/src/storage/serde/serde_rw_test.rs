#[cfg(test)]
mod test {
    use crate::storage::serde::{Datum, DatumReader, DatumWriter, OptDatum, ReadResult};
    use anyhow::{anyhow, Result};
    use itertools::Itertools;
    use rand::seq::SliceRandom;
    use std::io::{BufReader, BufWriter, Cursor};

    fn verify(pre_serialized: &Vec<OptDatum<Datum>>) -> Result<()> {
        let mut serialized: Vec<u8> = vec![];
        let mut w_len_at_each_dat: Vec<usize> = vec![]; // Cumulative w_len.
        {
            let mut w = DatumWriter::from(BufWriter::new(Cursor::new(&mut serialized)));
            let mut w_len = 0;
            for optdat in pre_serialized.iter() {
                let delta_w_len = w.ser_optdat(optdat)?;
                w_len += *delta_w_len;
                w_len_at_each_dat.push(w_len);
            }
            drop(w);
            assert_eq!(
                serialized.len(),
                w_len,
                "\n{:?}\n{:?}\n",
                pre_serialized,
                serialized
            );
        }

        {
            let mut r = DatumReader::from(BufReader::new(Cursor::new(&mut serialized)));
            let mut r_len = 0;
            for dat_i in 0..pre_serialized.len() {
                match r.skip()? {
                    ReadResult::EOF => return Err(anyhow!("Premature EOF")),
                    ReadResult::Some(delta_r_len, ()) => r_len += delta_r_len,
                }
                assert_eq!(w_len_at_each_dat[dat_i], r_len);
            }
            assert_eq!(
                ReadResult::EOF,
                r.deser()?,
                "\n{:?}\n{:?}\n",
                pre_serialized,
                serialized
            );
        }

        {
            let mut r = DatumReader::from(BufReader::new(Cursor::new(&mut serialized)));
            let mut r_len = 0;
            let mut deserialized: Vec<OptDatum<Datum>> = vec![];
            for dat_i in 0..pre_serialized.len() {
                match r.deser()? {
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
                r.deser()?,
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
    fn gen_tup_depth1() -> OptDatum<Datum> {
        OptDatum::Some(Datum::Tuple(vec![Datum::Str(String::from("asdf"))]))
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
            gen_tup_depth1,
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
