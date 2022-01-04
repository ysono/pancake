use anyhow::Result;
use async_trait::async_trait;
use pancake::storage::engine_serial::db::DB as SerialDb;
use pancake::storage::engine_ssi::oper::scnd_idx_mod::{
    self, CreateScndIdxResult, DeleteScndIdxResult,
};
use pancake::storage::engine_ssi::oper::txn::{CloseResult, CommitResult, Txn};
use pancake::storage::engine_ssi::DB as SsiDb;
use pancake::storage::types::{PKShared, PVShared, PrimaryKey, SubValue, SubValueSpec};
use std::sync::Arc;

/// Adaptor for different implementations of db engines.
///
/// For "get" operations, don't return Entries, but take data from Entries into owned forms.
/// This is purely for tester convenience.
#[async_trait]
pub trait OneStmtDbAdaptor {
    async fn get_pk_one(&self, pk: &PrimaryKey) -> Result<Option<(PKShared, PVShared)>>;

    async fn get_pk_range(
        &self,
        pk_lo: Option<&PrimaryKey>,
        pk_hi: Option<&PrimaryKey>,
    ) -> Result<Vec<(PKShared, PVShared)>>;

    async fn get_sv_range(
        &self,
        spec: &SubValueSpec,
        sv_lo: Option<&SubValue>,
        sv_hi: Option<&SubValue>,
    ) -> Result<Vec<(PKShared, PVShared)>>;

    async fn put(&mut self, pk: PKShared, pv: Option<PVShared>) -> Result<()>;

    async fn create_scnd_idx(&mut self, spec: Arc<SubValueSpec>) -> Result<()>;

    async fn delete_scnd_idx(&mut self, spec: &SubValueSpec) -> Result<()>;
}

pub struct OneStmtSerialDbAdaptor<'a> {
    pub db: &'a mut SerialDb,
}

#[async_trait]
impl<'a> OneStmtDbAdaptor for OneStmtSerialDbAdaptor<'a> {
    async fn get_pk_one(&self, pk: &PrimaryKey) -> Result<Option<(PKShared, PVShared)>> {
        let opt_entry = self.db.get_pk_one(pk);
        let opt_res = opt_entry.map(|entry| entry.take_kv());
        opt_res.transpose()
    }

    async fn get_pk_range(
        &self,
        pk_lo: Option<&PrimaryKey>,
        pk_hi: Option<&PrimaryKey>,
    ) -> Result<Vec<(PKShared, PVShared)>> {
        self.db
            .get_pk_range(pk_lo, pk_hi)
            .map(|entry| entry.take_kv())
            .collect::<Result<Vec<_>>>()
    }

    async fn get_sv_range(
        &self,
        spec: &SubValueSpec,
        sv_lo: Option<&SubValue>,
        sv_hi: Option<&SubValue>,
    ) -> Result<Vec<(PKShared, PVShared)>> {
        self.db
            .get_sv_range(spec, sv_lo, sv_hi)?
            .map(|entry| entry.take_kv())
            .collect::<Result<Vec<_>>>()
    }

    async fn put(&mut self, pk: PKShared, pv: Option<PVShared>) -> Result<()> {
        self.db.put(pk, pv)
    }

    async fn create_scnd_idx(&mut self, spec: Arc<SubValueSpec>) -> Result<()> {
        self.db.create_scnd_idx(spec)
    }

    async fn delete_scnd_idx(&mut self, spec: &SubValueSpec) -> Result<()> {
        self.db.delete_scnd_idx(spec)
    }
}

pub struct OneStmtSsiDbAdaptor<'a> {
    pub db: &'a SsiDb,
}

impl<'a> OneStmtSsiDbAdaptor<'a> {
    pub async fn nonmut_put(&self, pk: PKShared, pv: Option<PVShared>) -> Result<()> {
        let fut = Txn::run(self.db, |mut txn| {
            Box::pin(async {
                let res: Result<()> = async {
                    loop {
                        txn.put(pk.clone(), pv.clone()).await?;
                        match txn.try_commit().await? {
                            CommitResult::Conflict => txn.clear().await?,
                            CommitResult::Success => break,
                        }
                    }
                    Ok(())
                }
                .await;
                txn.close(res).await
            })
        });
        let res: CloseResult<()> = fut.await;
        res.into()
    }

    pub async fn nonmut_create_scnd_idx(&self, spec: Arc<SubValueSpec>) -> Result<()> {
        let fut = scnd_idx_mod::create_scnd_idx(self.db, spec);
        let res: Result<CreateScndIdxResult> = fut.await;
        res?;
        Ok(())
    }

    pub async fn nonmut_delete_scnd_idx(&self, spec: &SubValueSpec) -> Result<()> {
        let fut = scnd_idx_mod::delete_scnd_idx(self.db, spec);
        let res: Result<DeleteScndIdxResult> = fut.await;
        res?;
        Ok(())
    }
}

#[async_trait]
impl<'a> OneStmtDbAdaptor for OneStmtSsiDbAdaptor<'a> {
    async fn get_pk_one(&self, pk: &PrimaryKey) -> Result<Option<(PKShared, PVShared)>> {
        let fut = Txn::run(self.db, |mut txn| {
            Box::pin(async {
                let res = txn.get_pk_one(pk).await;
                txn.close(res).await
            })
        });
        let res: CloseResult<Option<(PKShared, PVShared)>> = fut.await;
        res.into()
    }

    async fn get_pk_range(
        &self,
        pk_lo: Option<&PrimaryKey>,
        pk_hi: Option<&PrimaryKey>,
    ) -> Result<Vec<(PKShared, PVShared)>> {
        let fut = Txn::run(self.db, |mut txn| {
            Box::pin(async {
                let res = txn
                    .get_pk_range(pk_lo, pk_hi, |entries| {
                        entries
                            .map(|entry| entry.take_kv())
                            .collect::<Result<Vec<_>>>()
                    })
                    .await;
                txn.close(res).await
            })
        });
        let res: CloseResult<Vec<(PKShared, PVShared)>> = fut.await;
        res.into()
    }

    async fn get_sv_range(
        &self,
        spec: &SubValueSpec,
        sv_lo: Option<&SubValue>,
        sv_hi: Option<&SubValue>,
    ) -> Result<Vec<(PKShared, PVShared)>> {
        let fut = Txn::run(self.db, |mut txn| {
            Box::pin(async {
                let res = txn
                    .get_sv_range(spec, sv_lo, sv_hi, |entries| {
                        entries
                            .map(|entry| entry.convert::<PKShared, PVShared>())
                            .map(|entry| entry.take_kv())
                            .collect::<Result<Vec<_>>>()
                    })
                    .await;
                txn.close(res).await
            })
        });
        let res: CloseResult<Vec<(PKShared, PVShared)>> = fut.await;
        res.into()
    }

    async fn put(&mut self, pk: PKShared, pv: Option<PVShared>) -> Result<()> {
        self.nonmut_put(pk, pv).await
    }

    async fn create_scnd_idx(&mut self, spec: Arc<SubValueSpec>) -> Result<()> {
        self.nonmut_create_scnd_idx(spec).await
    }

    async fn delete_scnd_idx(&mut self, spec: &SubValueSpec) -> Result<()> {
        self.nonmut_delete_scnd_idx(spec).await
    }
}
