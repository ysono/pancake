use crate::{
    lsm::{unit::CommitVer, ListVer},
    opers::txn::Txn,
};
use tokio::sync::mpsc::error::TrySendError;

impl<'txn> Txn<'txn> {
    pub(super) fn notify_fc_worker<const LEN: usize>(
        &self,
        updated_mhlv: Option<ListVer>,
        fc_able_commit_vers: [Option<CommitVer>; LEN],
    ) {
        /* Send info about min_held_list_ver first, b/c it's cheaper for the F+C worker to process. */
        self.send_updated_min_held_list_ver(updated_mhlv);
        self.send_fc_able_commit_vers(fc_able_commit_vers);
    }
    fn send_updated_min_held_list_ver(&self, updated_mhlv: Option<ListVer>) {
        if let Some(mhlv) = updated_mhlv {
            self.db.notify_min_held_list_ver(mhlv);
        }
    }
    fn send_fc_able_commit_vers<const LEN: usize>(
        &self,
        fc_able_commit_vers: [Option<CommitVer>; LEN],
    ) {
        for commit_ver in fc_able_commit_vers {
            if let Some(commit_ver) = commit_ver {
                let send_res = self.db.fc_able_commit_vers_tx().try_send(commit_ver);
                match send_res {
                    Err(TrySendError::Full(_)) => {
                        /* No-op. This log is too verbose unless debugging. */
                        // println!("fc_able_commit_vers channel is at capacity.");
                    }
                    Err(TrySendError::Closed(_)) => {}
                    Ok(()) => {}
                }
            }
        }
    }
}
