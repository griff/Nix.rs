use std::rc::Rc;

use capnp_convert::BuildFrom as _;
use nixrs::hash::NarHash;
use nixrs::store_path::StorePath;

use crate::capnp::nix_daemon_capnp::nix_daemon;
use crate::capnp::nixrs_capnp::nar;

pub struct DaemonNar {
    pub store: nix_daemon::Client,
    pub store_path: Rc<StorePath>,
    pub nar_hash: NarHash,
    pub nar_size: u64,
}

impl nar::Server for DaemonNar {
    async fn write_to(
        self: Rc<Self>,
        params: nar::WriteToParams,
        _result: nar::WriteToResults,
    ) -> capnp::Result<()> {
        let stream = params.get()?.get_stream()?;
        let mut req = self.store.nar_from_path_request();
        let mut b = req.get();
        b.reborrow().init_path().build_from(&*self.store_path)?;
        b.set_stream(stream);
        req.send().promise.await?;
        Ok(())
    }

    async fn nar_hash(
        self: Rc<Self>,
        _params: nar::NarHashParams,
        mut result: nar::NarHashResults,
    ) -> capnp::Result<()> {
        result.get().set_hash(self.nar_hash.digest_bytes());
        Ok(())
    }

    async fn nar_size(
        self: Rc<Self>,
        _params: nar::NarSizeParams,
        mut result: nar::NarSizeResults,
    ) -> capnp::Result<()> {
        result.get().set_size(self.nar_size);
        Ok(())
    }
}
