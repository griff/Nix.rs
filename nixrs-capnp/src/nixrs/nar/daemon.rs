use std::rc::Rc;

use capnp::Error as CapError;
use capnp::capability::Promise;
use capnp_rpc::pry;
use futures::TryFutureExt;
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
    fn write_to(
        &mut self,
        params: nar::WriteToParams,
        _result: nar::WriteToResults,
    ) -> Promise<(), CapError> {
        let stream = pry!(pry!(params.get()).get_stream());
        let mut req = self.store.nar_from_path_request();
        let mut b = req.get();
        pry!(b.set_path(&*self.store_path));
        b.set_stream(stream);
        Promise::from_future(req.send().promise.map_ok(|_| ()))
    }

    fn nar_hash(
        &mut self,
        _params: nar::NarHashParams,
        mut result: nar::NarHashResults,
    ) -> Promise<(), CapError> {
        result.get().set_hash(self.nar_hash.digest_bytes());
        Promise::ok(())
    }

    fn nar_size(
        &mut self,
        _params: nar::NarSizeParams,
        mut result: nar::NarSizeResults,
    ) -> Promise<(), CapError> {
        result.get().set_size(self.nar_size);
        Promise::ok(())
    }
}
