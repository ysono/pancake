use crate::ds_n_a::interval_set::{Interval, IntervalSet};
use crate::{
    db_state::ScndIdxState,
    lsm::{entryset::merging, unit::StagingUnit},
    opers::txn::Txn,
};
use anyhow::{anyhow, Result};
use pancake_engine_common::Entry;
use pancake_types::serde::OptDatum;
use pancake_types::types::{PKShared, PVShared, PrimaryKey, SVPKShared, SubValue, SubValueSpec};

impl<'txn> Txn<'txn> {
    pub fn get_pk_one(&mut self, pk: &'txn PrimaryKey) -> Result<Option<(PKShared, PVShared)>> {
        self.dependent_itvs_prim.add(Interval {
            lo_incl: Some(pk),
            hi_incl: Some(pk),
        });

        if let Some(stg) = self.staging.as_ref() {
            if let Some((pk, opt_pv)) = stg.prim.r_memlog().get_one(pk) {
                let opt_pv: Option<PVShared> = opt_pv.clone().into();
                let opt_pkpv = opt_pv.map(|pv| (pk.clone(), pv));
                return Ok(opt_pkpv);
            }
        }

        let committed_entrysets = self.snap.iter().filter_map(|unit| unit.prim.as_ref());
        for entryset in committed_entrysets {
            let gotten = entryset.get_one(pk);
            if let Some(entry) = gotten {
                return entry
                    .to_option_entry()
                    .map(|entry| entry.into_owned_kv())
                    .transpose();
            }
        }

        return Ok(None);
    }

    pub fn get_pk_range(
        &mut self,
        pk_lo: Option<&'txn PrimaryKey>,
        pk_hi: Option<&'txn PrimaryKey>,
    ) -> impl Iterator<Item = Entry<PKShared, PVShared>> {
        self.dependent_itvs_prim.add(Interval {
            lo_incl: pk_lo,
            hi_incl: pk_hi,
        });

        let stg = self.staging.as_ref().map(|stg| &stg.prim);
        let committed_entrysets = self.snap.iter().filter_map(|unit| unit.prim.as_ref());
        let kmerged_entries =
            merging::merge_txnlocal_and_committed_entrysets(stg, committed_entrysets, pk_lo, pk_hi);
        let non_tomb_entries = kmerged_entries.filter_map(|entry| entry.to_option_entry());
        non_tomb_entries
    }

    pub fn get_sv_range(
        &mut self,
        sv_spec_arg: &SubValueSpec,
        sv_lo: Option<&'txn SubValue>,
        sv_hi: Option<&'txn SubValue>,
    ) -> Result<impl Iterator<Item = Entry<SVPKShared, PVShared>>> {
        let ScndIdxState {
            scnd_idx_num,
            is_readable,
        } = self
            .db_state_guard
            .scnd_idxs()
            .get(sv_spec_arg)
            .ok_or_else(|| anyhow!("Secondary index does not exist for {sv_spec_arg:?}"))?;
        if is_readable == &false {
            return Err(anyhow!(
                "Secondary index for {sv_spec_arg:?} has not finished building",
            ));
        }

        let itvset = self
            .dependent_itvs_scnds
            .entry(*scnd_idx_num)
            .or_insert_with(IntervalSet::new);
        itvset.add(Interval {
            lo_incl: sv_lo,
            hi_incl: sv_hi,
        });

        let stg = self
            .staging
            .as_ref()
            .and_then(|stg| stg.scnds.get(scnd_idx_num));
        let committed_entrysets = self
            .snap
            .iter()
            .filter_map(|unit| unit.scnds.get(scnd_idx_num));
        let kmerged_entries =
            merging::merge_txnlocal_and_committed_entrysets(stg, committed_entrysets, sv_lo, sv_hi);
        let non_tomb_entries = kmerged_entries.filter_map(|entry| entry.to_option_entry());
        Ok(non_tomb_entries)
    }

    pub fn put(&mut self, pk: &'txn PKShared, new_pv: &Option<PVShared>) -> Result<()> {
        let old_pkpv = self.get_pk_one(pk)?;
        let old_pv = old_pkpv.map(|(_, pv)| pv);

        self.ensure_create_staging()?;

        self.put_scnd_stg_delta(pk, &old_pv, new_pv)?;

        let new_pv = OptDatum::<PVShared>::from(new_pv.clone());
        let stg = self.staging.as_mut().unwrap();
        stg.prim.put(pk.clone(), new_pv)?;

        Ok(())
    }

    fn put_scnd_stg_delta(
        &mut self,
        pk: &'txn PKShared,
        old_pv: &Option<PVShared>,
        new_pv: &Option<PVShared>,
    ) -> Result<()> {
        let stg = self.staging.as_mut().unwrap();

        for (sv_spec, ScndIdxState { scnd_idx_num, .. }) in self.db_state_guard.scnd_idxs().iter() {
            let old_sv = old_pv.as_ref().and_then(|pv| sv_spec.extract(pv));
            let new_sv = new_pv.as_ref().and_then(|pv| sv_spec.extract(pv));

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
                let stg_scnd = stg.ensure_create_scnd(*scnd_idx_num)?;
                stg_scnd.put(
                    SVPKShared {
                        sv: old_sv,
                        pk: pk.clone(),
                    },
                    OptDatum::Tombstone,
                )?;
            }
            if let Some(new_sv) = new_sv {
                let stg_scnd = stg.ensure_create_scnd(*scnd_idx_num)?;
                stg_scnd.put(
                    SVPKShared {
                        sv: new_sv,
                        pk: pk.clone(),
                    },
                    OptDatum::Some(new_pv.clone().unwrap()),
                )?;
            }
        }

        Ok(())
    }

    fn ensure_create_staging(&mut self) -> Result<()> {
        if self.staging.is_none() {
            let unit_dir = self.db.lsm_dir().format_new_unit_dir_path();
            let stg = StagingUnit::new_empty(unit_dir)?;
            self.staging = Some(stg);
        }
        Ok(())
    }
}
