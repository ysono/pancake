use anyhow::Result;
use async_trait::async_trait;
use pancake::storage::engine_serial::db::DB as SerialDb;
use pancake::storage::engine_ssi::{ClientCommitDecision, Txn, DB as SsiDb};
use pancake::storage::types::{PKShared, PVShared, PrimaryKey, SubValue, SubValueSpec};
use std::sync::Arc;

/// Adaptor for different implementations of db engines.
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
        sv_spec: &SubValueSpec,
        sv_lo: Option<&SubValue>,
        sv_hi: Option<&SubValue>,
    ) -> Result<Vec<(PKShared, PVShared)>>;

    async fn put(&mut self, pk: PKShared, pv: Option<PVShared>) -> Result<()>;

    async fn create_scnd_idx(&mut self, sv_spec: Arc<SubValueSpec>) -> Result<()>;

    async fn delete_scnd_idx(&mut self, sv_spec: &SubValueSpec) -> Result<()>;
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
        sv_spec: &SubValueSpec,
        sv_lo: Option<&SubValue>,
        sv_hi: Option<&SubValue>,
    ) -> Result<Vec<(PKShared, PVShared)>> {
        self.db
            .get_sv_range(sv_spec, sv_lo, sv_hi)?
            .map(|entry| entry.take_kv())
            .collect::<Result<Vec<_>>>()
    }

    async fn put(&mut self, pk: PKShared, pv: Option<PVShared>) -> Result<()> {
        self.db.put(pk, pv)
    }

    async fn create_scnd_idx(&mut self, sv_spec: Arc<SubValueSpec>) -> Result<()> {
        self.db.create_scnd_idx(sv_spec)
    }

    async fn delete_scnd_idx(&mut self, sv_spec: &SubValueSpec) -> Result<()> {
        self.db.delete_scnd_idx(sv_spec)
    }
}

pub struct OneStmtSsiDbAdaptor<'a> {
    pub db: &'a SsiDb,
}

impl<'a> OneStmtSsiDbAdaptor<'a> {
    pub async fn nonmut_put(&self, pk: PKShared, pv: Option<PVShared>) -> Result<()> {
        let fut = Txn::run(self.db, 0, |txn| {
            txn.put(&pk, &pv)?;
            Ok(ClientCommitDecision::Commit(()))
        });
        let res = fut.await;
        res
    }

    pub async fn nonmut_create_scnd_idx(&self, sv_spec: Arc<SubValueSpec>) -> Result<()> {
        self.db.create_scnd_idx(&sv_spec).await
    }

    pub async fn nonmut_delete_scnd_idx(&self, sv_spec: &SubValueSpec) -> Result<()> {
        self.db.delete_scnd_idx(sv_spec).await
    }
}

#[async_trait]
impl<'a> OneStmtDbAdaptor for OneStmtSsiDbAdaptor<'a> {
    async fn get_pk_one(&self, pk: &PrimaryKey) -> Result<Option<(PKShared, PVShared)>> {
        let fut = Txn::run(self.db, 0, |txn| {
            let opt_pkpv = txn.get_pk_one(pk)?;
            Ok(ClientCommitDecision::Commit(opt_pkpv))
        });
        let res = fut.await;
        res
    }

    async fn get_pk_range(
        &self,
        pk_lo: Option<&PrimaryKey>,
        pk_hi: Option<&PrimaryKey>,
    ) -> Result<Vec<(PKShared, PVShared)>> {
        let fut = Txn::run(self.db, 0, |txn| {
            let entries = txn.get_pk_range(pk_lo, pk_hi);
            let entries = entries
                .map(|entry| entry.take_kv())
                .collect::<Result<Vec<_>>>()?;
            Ok(ClientCommitDecision::Commit(entries))
        });
        let res = fut.await;
        res
    }

    async fn get_sv_range(
        &self,
        sv_spec: &SubValueSpec,
        sv_lo: Option<&SubValue>,
        sv_hi: Option<&SubValue>,
    ) -> Result<Vec<(PKShared, PVShared)>> {
        let fut = Txn::run(self.db, 0, |txn| {
            let entries = txn.get_sv_range(sv_spec, sv_lo, sv_hi)?;
            let entries = entries
                .map(|entry| entry.convert::<PKShared, PVShared>().take_kv())
                .collect::<Result<Vec<_>>>()?;
            Ok(ClientCommitDecision::Commit(entries))
        });
        let res = fut.await;
        res
    }

    async fn put(&mut self, pk: PKShared, pv: Option<PVShared>) -> Result<()> {
        self.nonmut_put(pk, pv).await
    }

    async fn create_scnd_idx(&mut self, sv_spec: Arc<SubValueSpec>) -> Result<()> {
        self.nonmut_create_scnd_idx(sv_spec).await
    }

    async fn delete_scnd_idx(&mut self, sv_spec: &SubValueSpec) -> Result<()> {
        self.nonmut_delete_scnd_idx(sv_spec).await
    }
}
