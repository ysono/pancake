use super::Txn;
use crate::ds_n_a::interval_set::{Interval, IntervalSet};
use crate::storage::engines_common::Entry;
use crate::storage::serde::OptDatum;
use crate::storage::types::{
    PKShared, PVShared, PrimaryKey, SVPKShared, SVShared, SubValue, SubValueSpec,
};
use anyhow::{anyhow, Result};
use std::sync::atomic::Ordering;
use std::sync::Arc;

impl<'txn> Txn<'txn> {
    pub async fn get_pk_one(
        &mut self,
        pk: &'txn PrimaryKey,
    ) -> Result<Option<(PKShared, PVShared)>> {
        self.dep_itvs_prim.add(Interval {
            lo_incl: Some(pk),
            hi_incl: Some(pk),
        });

        self.db
            .prim_lsm()
            .get_one(
                self.written_prim.as_ref(),
                Some(self.snap_ver_ceil),
                None,
                pk,
                || self.db.send_job_cv(),
            )
            .await
    }

    pub async fn get_pk_range<Cb, CbRet>(
        &mut self,
        pk_lo: Option<&'txn PrimaryKey>,
        pk_hi: Option<&'txn PrimaryKey>,
        cb: Cb,
    ) -> CbRet
    where
        Cb: FnMut(&mut dyn Iterator<Item = Entry<PKShared, PVShared>>) -> CbRet,
    {
        self.dep_itvs_prim.add(Interval {
            lo_incl: pk_lo,
            hi_incl: pk_hi,
        });

        let cb_ret: CbRet = self
            .db
            .prim_lsm()
            .get_range(
                self.written_prim.as_ref(),
                Some(self.snap_ver_ceil),
                None,
                pk_lo,
                pk_hi,
                cb,
                || self.db.send_job_cv(),
            )
            .await;
        cb_ret
    }

    pub async fn get_sv_range<Cb, CbRetOk>(
        &mut self,
        spec_arg: &'txn SubValueSpec,
        sv_lo: Option<&'txn SubValue>,
        sv_hi: Option<&'txn SubValue>,
        cb: Cb,
    ) -> Result<CbRetOk>
    where
        Cb: FnMut(&mut dyn Iterator<Item = Entry<SVPKShared, PVShared>>) -> Result<CbRetOk>,
    {
        let spec: &Arc<SubValueSpec>;
        let lsm;
        match self.scnd_idxs_guard.get_key_value(spec_arg) {
            None => return Err(anyhow!("Secondary index does not exist for {:?}", spec_arg)),
            Some((spec_shared, scnd_idx)) => {
                if scnd_idx.is_built().load(Ordering::SeqCst) == false {
                    return Err(anyhow!(
                        "Required secondary index has not finished building"
                    ));
                } else {
                    spec = spec_shared;
                    lsm = scnd_idx.lsm();
                }
            }
        };

        if !self.dep_itvs_scnds.contains_key(spec) {
            self.dep_itvs_scnds
                .insert(Arc::clone(spec), IntervalSet::new());
        }
        let dep_itvs = self.dep_itvs_scnds.get_mut(spec).unwrap();
        dep_itvs.add(Interval {
            lo_incl: sv_lo,
            hi_incl: sv_hi,
        });

        let cb_ret: Result<CbRetOk> = lsm
            .get_range(
                self.written_scnds.get(spec),
                Some(self.snap_ver_ceil),
                None,
                sv_lo,
                sv_hi,
                cb,
                || self.db.send_job_cv(),
            )
            .await;
        cb_ret
    }

    pub async fn put(&mut self, pk: PKShared, new_pv: Option<PVShared>) -> Result<()> {
        self.create_written().await?;

        let old_pkv = self
            .db
            .prim_lsm()
            .get_one(
                self.written_prim.as_ref(),
                Some(self.snap_ver_ceil),
                None,
                &pk,
                || self.db.send_job_cv(),
            )
            .await?;
        let old_pv: Option<PVShared> = old_pkv.map(|(_, pv)| pv);

        for (spec, written) in self.written_scnds.iter_mut() {
            let old_sv: Option<SVShared> = old_pv.as_ref().and_then(|pv| spec.extract(pv));
            let new_sv: Option<SVShared> = new_pv.as_ref().and_then(|pv| spec.extract(pv));

            // Assign old_sv to be Some iff we need to tombstone the old entry.
            // Assign new_sv to be Some iff we need to put the new entry.
            let (old_sv, new_sv) = match (old_sv, new_sv) {
                (Some(old_sv), Some(new_sv)) => {
                    if old_sv != new_sv {
                        (Some(old_sv), Some(new_sv))
                    } else if old_pv != new_pv {
                        (None, Some(new_sv))
                    } else {
                        (None, None)
                    }
                }
                pair => pair,
            };

            if let Some(old_sv) = old_sv {
                written.put(
                    SVPKShared {
                        sv: old_sv,
                        pk: pk.clone(),
                    },
                    OptDatum::Tombstone,
                )?;
            }
            if let Some(new_sv) = new_sv {
                written.put(
                    SVPKShared {
                        sv: new_sv,
                        pk: pk.clone(),
                    },
                    OptDatum::Some(new_pv.clone().unwrap()),
                )?;
            }
        }

        let new_pv = OptDatum::<PVShared>::from(new_pv);
        let written_prim = self.written_prim.as_mut().unwrap();
        written_prim.put(pk, new_pv)?;

        Ok(())
    }

    async fn create_written(&mut self) -> Result<()> {
        if self.written_prim.is_none() {
            let written_prim = self.db.prim_lsm().create_writable_entryset().await?;
            self.written_prim = Some(written_prim);

            for (spec, scnd_idx) in self.scnd_idxs_guard.iter() {
                let written_scnd = scnd_idx.lsm().create_writable_entryset().await?;
                self.written_scnds.insert(spec.clone(), written_scnd);
            }
        }

        Ok(())
    }
}
