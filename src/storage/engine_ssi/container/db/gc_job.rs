use crate::storage::engine_ssi::container::{LSMTree, DB};
use crate::storage::serde::{OptDatum, Serializable};
use anyhow::Result;
use std::future::Future;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::watch;

impl DB {
    pub fn load_db_and_gc_job<P: AsRef<Path>>(
        db_dir_path: P,
    ) -> Result<(Arc<DB>, impl Future<Output = Result<()>>)> {
        let db = Self::load_or_new(db_dir_path)?;
        let db = Arc::new(db);
        let job_fut = Arc::clone(&db).run_gc_job();
        Ok((db, job_fut))
    }

    async fn run_gc_job(self: Arc<DB>) -> Result<()> {
        let mut job_cv_rx = self.job_cv_tx().subscribe();

        loop {
            if self.is_terminating.load(Ordering::SeqCst) == true {
                break;
            }

            self.on_loop(&self.prim_lsm).await?;
            {
                let scnd_idxs_guard = self.scnd_idxs.read().await;
                for (_spec, scnd_idx) in scnd_idxs_guard.iter() {
                    self.on_loop(scnd_idx.lsm()).await?;
                }
            }

            job_cv_rx.changed().await?;
        }

        Self::on_termination(&self.prim_lsm, &mut job_cv_rx).await?;
        {
            let scnd_idxs_guard = self.scnd_idxs.read().await;
            for (_spec, scnd_idx) in scnd_idxs_guard.iter() {
                Self::on_termination(scnd_idx.lsm(), &mut job_cv_rx).await?;
            }
        }

        Ok(())
    }

    async fn on_loop<K, V>(&self, lsm: &LSMTree<K, V>) -> Result<()>
    where
        K: Serializable + Ord + Clone,
        OptDatum<V>: Serializable,
    {
        lsm.delete_dangling_slices()?;

        let min_separate_commit_ver = self.commit_ver_state().trailing();

        lsm.modify_linked_list(min_separate_commit_ver).await?;

        /* Immediately try to delete dangling slices, which might include
        those that just became dangling. */
        lsm.delete_dangling_slices()?;

        Ok(())
    }

    async fn on_termination<K, V>(
        lsm: &LSMTree<K, V>,
        job_cv_rx: &mut watch::Receiver<()>,
    ) -> Result<()> {
        loop {
            lsm.delete_dangling_slices()?;
            if lsm.is_cleanup_done() {
                break;
            } else {
                job_cv_rx.changed().await?;
            }
        }
        Ok(())
    }
}
