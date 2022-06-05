use crate::ds_n_a::atomic_linked_list::{ListElem, ListNode};
use crate::ds_n_a::send_ptr::SendPtr;
use crate::storage::engine_ssi::container::{SecondaryIndex, DB};
use crate::storage::engine_ssi::entryset::{
    CommitInfo, CommitVer, CommittedEntrySet, CommittedEntrySetInfo, SSTable, Timestamp,
    SCND_IDX_ASYNC_BUILT_COMMIT_VER,
};
use crate::storage::engines_common::Entry;
use crate::storage::serde::OptDatum;
use crate::storage::types::{PKShared, PVShared, SVPKShared, SubValueSpec};
use anyhow::{anyhow, Result};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

pub enum CreateScndIdxResult {
    NoOp(String),
    Success,
}

pub async fn create_scnd_idx(db: &DB, spec: Arc<SubValueSpec>) -> Result<CreateScndIdxResult> {
    {
        let scnd_idxs_guard = db.scnd_idxs().read().await;
        if let Some(scnd_idx) = scnd_idxs_guard.get(&spec) {
            let is_built = scnd_idx.is_built().load(Ordering::SeqCst);
            return Ok(CreateScndIdxResult::NoOp(format!(
                "Secondary index for spec {:?} already exists w/ is_built={}.",
                spec, is_built
            )));
        }
    }

    let newer_outofscope_node: SendPtr<ListNode<_>>;
    let job_ver_ceil: CommitVer;
    {
        let mut scnd_idxs_guard = db.scnd_idxs().write().await;
        if let Some(scnd_idx) = scnd_idxs_guard.get(&spec) {
            let is_built = scnd_idx.is_built().load(Ordering::SeqCst);
            return Ok(CreateScndIdxResult::NoOp(format!(
                "Secondary index for spec {:?} already exists w/ is_built={}.",
                spec, is_built
            )));
        }

        let scnd_idx_dir = db.format_new_scnd_idx_dir_path().await?;
        let scnd_idx = SecondaryIndex::new(scnd_idx_dir, Arc::clone(&spec))?;

        /* Push a terminus dummy in order to prevent the GC job from traversing older than there. */
        let oos_elem = ListElem::new_dummy(true);
        let oos_node = scnd_idx.lsm().list().push_newest(oos_elem);
        newer_outofscope_node = SendPtr::from(oos_node);

        scnd_idxs_guard.insert(Arc::clone(&spec), scnd_idx);

        /* Get the current leading commit ver, but don't `hold()` it.
        It's possible for this job to redundantly work on (commit vers >= job_ver_ceil),
        but it's better than blocking (commit vers >= job_ver_ceil) from being merged by the GC job. */
        job_ver_ceil = db.commit_ver_state().leading().await;
    }

    {
        let guard = db.scnd_idxs().read().await;
        let scnd_idx = guard.get(&spec).ok_or(
            /* This error shouldn't happen, because scnd idx deletion should abort
            if is_built == false. */
            anyhow!("Secondary index creation was interrupted. Try again."),
        )?;

        let entryset_dir = scnd_idx.lsm().format_new_entryset_dir_path().await?;
        let entryset_info = CommittedEntrySetInfo {
            commit_info: CommitInfo {
                commit_ver_hi_incl: SCND_IDX_ASYNC_BUILT_COMMIT_VER,
                commit_ver_lo_incl: SCND_IDX_ASYNC_BUILT_COMMIT_VER,
                timestamp: Timestamp::default(),
            },
            entryset_dir,
        };

        let sst = do_build(&db, job_ver_ceil, &spec, entryset_info).await?;

        if sst.is_empty()? {
            set_newer_oos_as_nonterminus(newer_outofscope_node);
            sst.remove_entryset_dir()?;
        } else {
            swap_in(scnd_idx, sst, newer_outofscope_node);
            set_newer_oos_as_nonterminus(newer_outofscope_node);
            wait_till_readable(&db, job_ver_ceil).await;
        }

        scnd_idx.is_built().store(true, Ordering::SeqCst);

        db.send_job_cv();
    }

    Ok(CreateScndIdxResult::Success)
}

async fn do_build(
    db: &DB,
    job_ver_ceil: CommitVer,
    spec: &SubValueSpec,
    entryset_info: CommittedEntrySetInfo,
) -> Result<SSTable<SVPKShared, PVShared>> {
    db.prim_lsm()
        .get_range(
            None,
            Some(job_ver_ceil),
            None,
            None::<&PKShared>,
            None::<&PKShared>,
            |prim_entries| -> Result<SSTable<SVPKShared, PVShared>> {
                // TODO
                // 1) flush to a new SSTable periodically.
                // 2) k-merge all SSTables into one SSTable.

                let mut memtable = BTreeMap::<SVPKShared, PVShared>::new();
                for prim_entry in prim_entries {
                    let (_, pv) = prim_entry.try_borrow()?;
                    if let Some(sv) = spec.extract(pv) {
                        let (pk, pv) = prim_entry.take_kv()?;
                        let svpk = SVPKShared { sv, pk };
                        memtable.insert(svpk, pv);
                    }
                }

                let output_entries = memtable
                    .into_iter()
                    .map(|(svpk, pv)| Entry::Own(Ok((svpk, OptDatum::Some(pv)))));

                SSTable::new(output_entries, entryset_info)
            },
            || db.send_job_cv(),
        )
        .await
}

fn swap_in(
    scnd_idx: &SecondaryIndex,
    sst: SSTable<SVPKShared, PVShared>,
    oos_ptr: SendPtr<ListNode<CommittedEntrySet<SVPKShared, PVShared>>>,
) {
    let elem = ListElem::Elem(CommittedEntrySet::SSTable(sst));
    let dummy_oldset = scnd_idx.lsm().list().dummy_oldest();
    let node = Box::new(ListNode {
        elem,
        older: AtomicPtr::new(dummy_oldset as *const _ as *mut _),
    });
    let node_ptr = Box::into_raw(node);

    let oos_ref = unsafe { oos_ptr.as_ref() };
    oos_ref.older.store(node_ptr, Ordering::SeqCst);
}

async fn wait_till_readable(db: &DB, job_ver_ceil: CommitVer) {
    let mut job_cv_rx = db.job_cv_tx().subscribe();
    loop {
        /* On the first run of the loop, read min_separate_commit_ver before waiting on job_cv_rx.
        This way, if min_separate_commit_ver may not have changed, we are not blocked forever. */
        let min_separate_commit_ver = db.commit_ver_state().trailing();
        if job_ver_ceil <= min_separate_commit_ver {
            break;
        }
        job_cv_rx.changed().await.ok();
    }
}

fn set_newer_oos_as_nonterminus(
    oos_ptr: SendPtr<ListNode<CommittedEntrySet<SVPKShared, PVShared>>>,
) {
    let oos_ref = unsafe { oos_ptr.as_ref() };
    if let ListElem::Dummy { is_terminus } = &oos_ref.elem {
        is_terminus.store(false, Ordering::SeqCst);
    }
}
