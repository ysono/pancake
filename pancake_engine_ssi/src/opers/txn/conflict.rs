use crate::{lsm_state::LsmElem, opers::txn::Txn};
use anyhow::Result;

impl<'txn> Txn<'txn> {
    pub(super) fn has_conflict(&mut self) -> Result<bool> {
        self.dependent_itvs_prim.merge();
        for (_, scnd_itvset) in self.dependent_itvs_scnds.iter_mut() {
            scnd_itvset.merge();
        }

        let committed_units = self.snap.iter().filter_map(|elem| match &elem {
            LsmElem::Dummy { .. } => None,
            LsmElem::Unit(unit) => Some(unit),
        });
        for unit in committed_units {
            if let Some(committed_prim) = unit.prim.as_ref() {
                let has_conflict = self
                    .dependent_itvs_prim
                    .overlaps_with(committed_prim.get_all_keys())?;
                if has_conflict {
                    return Ok(true);
                }
            }
            for (si_num, scnd_itvset) in self.dependent_itvs_scnds.iter() {
                if let Some(committed_scnd) = unit.scnds.get(si_num) {
                    let has_conflict = scnd_itvset.overlaps_with(committed_scnd.get_all_keys())?;
                    if has_conflict {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }
}
