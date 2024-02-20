use crate::opers::txn::Txn;
use anyhow::Result;

impl<'txn> Txn<'txn> {
    pub(super) fn has_conflict(&mut self) -> Result<bool> {
        let dep_itvs_prim = self.dependent_itvs_prim.merge();
        let dep_scnds = self
            .dependent_itvs_scnds
            .iter_mut()
            .map(|(si_num, scnd_itvset)| {
                let itvs_scnd = scnd_itvset.merge();
                (*si_num, itvs_scnd)
            })
            .collect::<Vec<_>>();

        for unit in self.snap.iter() {
            if let Some(committed_prim) = unit.prim.as_ref() {
                let has_conflict = dep_itvs_prim.overlaps_with(committed_prim.get_all_keys())?;
                if has_conflict {
                    return Ok(true);
                }
            }
            for (si_num, dep_itvs_scnd) in dep_scnds.iter() {
                if let Some(committed_scnd) = unit.scnds.get(si_num) {
                    let has_conflict =
                        dep_itvs_scnd.overlaps_with(committed_scnd.get_all_keys())?;
                    if has_conflict {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }
}
