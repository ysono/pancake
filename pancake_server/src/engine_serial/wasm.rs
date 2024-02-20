use anyhow::{anyhow, Result};
use pancake_engine_serial::DB;
use pancake_types::types::{Deser, PrimaryKey, Ser, SubValue, SubValueSpec, Value};
use std::borrow::BorrowMut;
use std::sync::Arc;
use tokio::sync::RwLock;
use wit_bindgen_host_wasmtime_rust::wasmtime::{
    self,
    component::{Component, Linker},
    Config, Engine, Store,
};

wit_bindgen_host_wasmtime_rust::generate!({
    import: "./assets/db.wit",
    default: "./assets/udf.wit",
    name: "udf",
});
use db::{Pk, Pkpv, Pv, Sv, SvSpec};

pub struct WasmEngine {
    db: Arc<RwLock<DB>>,
    engine: Engine,
    linker: Linker<WasmState>,
}

impl WasmEngine {
    pub fn new(db: Arc<RwLock<DB>>) -> Result<Self> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        let engine = Engine::new(&config)?;

        let mut linker = Linker::new(&engine);
        db::add_to_linker(&mut linker, |state: &mut WasmState| &mut state.db_provider)?;

        Ok(Self { db, engine, linker })
    }

    pub async fn serve(&self, compo_bytes: &[u8]) -> Result<String> {
        // Coerce `&mut db` as `'static`.
        let mut db = self.db.write().await;
        let db: &mut DB = db.borrow_mut();
        let db = db as *mut DB;
        let db: &'static mut DB = unsafe { &mut *db };

        let state = WasmState {
            db_provider: DbProvider { db },
        };
        let mut store = Store::new(&self.engine, state);

        let compo = Component::new(&self.engine, compo_bytes)?;
        let (udf, _inst) = Udf::instantiate(&mut store, &compo, &self.linker)?;

        let res_commit_dec = udf.run_txn(&mut store)?;
        match res_commit_dec {
            Err(client_str) => Err(anyhow!(client_str)),
            Ok(CommitDecision::Abort(client_str)) => Err(anyhow!(
                "Aborting is not supported by the serial engine. Your txn's changes were already made. Wasm output: {client_str}", )),
            Ok(CommitDecision::Commit(client_str)) => Ok(client_str),
        }
    }
}

struct WasmState {
    db_provider: DbProvider,
}

struct DbProvider {
    /// Not actually static. Bound to the lifetime of the [`WasmEngine`] singleton.
    /// Making it static obviates making [`WasmEngine`] typed with a lifetime.
    /// Is there a better way?
    db: &'static mut DB,
}
impl db::Db for DbProvider {
    fn get_pk_one(&mut self, pk: Pk) -> anyhow::Result<Result<Option<Pkpv>, String>> {
        let pk = PrimaryKey::deser_solo(&pk.bytes)?;

        let opt_entry = self.db.get_pk_one(&pk);
        let opt_res_pkpv = opt_entry.map(|entry| -> Result<Pkpv> {
            let (pk, pv) = entry.try_borrow()?;
            let pk = pk.ser_solo()?;
            let pv = pv.ser_solo()?;
            let pk = Pk { bytes: pk };
            let pv = Pv { bytes: pv };
            Ok(Pkpv { pk, pv })
        });
        let res_opt_pkpv = opt_res_pkpv.transpose().map_err(|e| e.to_string());
        Ok(res_opt_pkpv)
    }

    fn get_pk_range(
        &mut self,
        pk_lo: Option<Pk>,
        pk_hi: Option<Pk>,
    ) -> anyhow::Result<Result<Vec<Pkpv>, String>> {
        let pk_lo = pk_lo
            .map(|pk| PrimaryKey::deser_solo(&pk.bytes))
            .transpose()?;
        let pk_hi = pk_hi
            .map(|pk| PrimaryKey::deser_solo(&pk.bytes))
            .transpose()?;

        let mut ret = vec![];
        for entry in self.db.get_pk_range(pk_lo.as_ref(), pk_hi.as_ref()) {
            let (pk, pv) = entry.try_borrow()?;
            let pk = pk.ser_solo()?;
            let pv = pv.ser_solo()?;
            let pk = Pk { bytes: pk };
            let pv = Pv { bytes: pv };
            ret.push(Pkpv { pk, pv });
        }
        Ok(Ok(ret))
    }

    fn get_sv_range(
        &mut self,
        sv_spec: SvSpec,
        sv_lo: Option<Sv>,
        sv_hi: Option<Sv>,
    ) -> anyhow::Result<Result<Vec<Pkpv>, String>> {
        let sv_spec = SubValueSpec::deser_solo(&sv_spec.bytes)?;
        let sv_lo = sv_lo
            .map(|sv| SubValue::deser_solo(&sv.bytes))
            .transpose()?;
        let sv_hi = sv_hi
            .map(|sv| SubValue::deser_solo(&sv.bytes))
            .transpose()?;

        let mut ret = vec![];
        for entry in self
            .db
            .get_sv_range(&sv_spec, sv_lo.as_ref(), sv_hi.as_ref())?
        {
            let (pk, pv) = entry.try_borrow()?;
            let pk = pk.ser_solo()?;
            let pv = pv.ser_solo()?;
            let pk = Pk { bytes: pk };
            let pv = Pv { bytes: pv };
            ret.push(Pkpv { pk, pv });
        }
        Ok(Ok(ret))
    }

    fn put(&mut self, pk: Pk, opt_pv: Option<Pv>) -> anyhow::Result<Result<(), String>> {
        let pk = PrimaryKey::deser_solo(&pk.bytes)?;
        let opt_pv = opt_pv.map(|pv| Value::deser_solo(&pv.bytes)).transpose()?;

        let pk = Arc::new(pk);
        let opt_pv = opt_pv.map(Arc::new);

        self.db.put(pk, opt_pv)?;

        Ok(Ok(()))
    }
}
