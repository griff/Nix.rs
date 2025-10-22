use capnp::{
    capability::{FromClientHook, Promise},
    traits::HasTypeId,
};
use capnp_rpc::pry;

use crate::capnp::lookup_capnp;

pub struct CapLookup {
    client: lookup_capnp::cap_lookup::Client,
}

impl CapLookup {
    pub fn new(client: lookup_capnp::cap_lookup::Client) -> Self {
        Self { client }
    }

    pub async fn lookup<C>(&self) -> capnp::Result<Option<C>>
    where
        C: FromClientHook + HasTypeId,
    {
        let mut req = self.client.lookup_request();
        req.get().init_priority(1).get(0).set_cap_type(C::TYPE_ID);
        let res = req.send().promise.await?;
        let r = res.get()?;
        if r.has_selected() {
            let cap_type = r.get_selected()?.get_cap_type();
            if cap_type != C::TYPE_ID {
                return Err(capnp::Error::failed(format!(
                    "Returned capability 0x{cap_type:x} does not match requested 0x{:x}",
                    C::TYPE_ID
                )));
            }
            Ok(Some(r.get_selected()?.get_cap().get_as_capability::<C>()?))
        } else {
            Ok(None)
        }
    }

    pub async fn required_lookup<C>(&self) -> capnp::Result<C>
    where
        C: FromClientHook + HasTypeId,
    {
        self.lookup::<C>().await?.ok_or_else(|| {
            capnp::Error::failed(format!("Missing required capability 0x{:x}", C::TYPE_ID))
        })
    }

    pub fn lookup_pipeline<C>(&self) -> capnp::Result<C>
    where
        C: FromClientHook + HasTypeId,
    {
        let mut req = self.client.lookup_request();
        req.get().init_priority(1).get(0).set_cap_type(C::TYPE_ID);
        let hook = req.send().pipeline.get_selected().get_cap().as_cap();
        Ok(C::new(hook))
    }
}

pub trait ParamsCap {
    fn cap_type(&self) -> u64;
    fn add_ref(&self) -> Box<dyn ParamsCap>;
    fn make_cap(
        &self,
        params: lookup_capnp::matcher::Reader,
    ) -> capnp::Result<Option<capnp::capability::Client>>;
}

struct LookupParamsCap {
    lookup: Box<dyn ParamsCap>,
}
impl Clone for LookupParamsCap {
    fn clone(&self) -> Self {
        Self {
            lookup: self.lookup.add_ref(),
        }
    }
}
impl ParamsCap for LookupParamsCap {
    fn cap_type(&self) -> u64 {
        self.lookup.cap_type()
    }

    fn add_ref(&self) -> Box<dyn ParamsCap> {
        self.lookup.add_ref()
    }

    fn make_cap(
        &self,
        params: lookup_capnp::matcher::Reader,
    ) -> capnp::Result<Option<capnp::capability::Client>> {
        self.lookup.make_cap(params)
    }
}
impl<C> ParamsCap for C
where
    C: HasTypeId + FromClientHook + 'static,
{
    fn cap_type(&self) -> u64 {
        C::TYPE_ID
    }

    fn add_ref(&self) -> Box<dyn ParamsCap> {
        Box::new(C::new(self.as_client_hook().add_ref()))
    }

    fn make_cap(
        &self,
        _params: lookup_capnp::matcher::Reader,
    ) -> capnp::Result<Option<capnp::capability::Client>> {
        Ok(Some(capnp::capability::Client::new(
            self.as_client_hook().add_ref(),
        )))
    }
}

#[derive(Default)]
pub struct CapRegistry {
    caps: Vec<LookupParamsCap>,
}

impl CapRegistry {
    pub fn new() -> Self {
        Default::default()
    }
    pub fn add_lookup<P>(&mut self, lookup: P)
    where
        P: ParamsCap + 'static,
    {
        self.caps.push(LookupParamsCap {
            lookup: Box::new(lookup),
        });
    }
}

impl lookup_capnp::cap_lookup::Server for CapRegistry {
    fn lookup(
        &mut self,
        params: lookup_capnp::cap_lookup::LookupParams,
        mut result: lookup_capnp::cap_lookup::LookupResults,
    ) -> Promise<(), capnp::Error> {
        let rl = pry!(pry!(params.get()).get_priority());
        for r in rl.iter() {
            let ty = r.get_cap_type();
            if let Some(cap) = self.caps.iter().find(|cap| ty == cap.cap_type()) {
                if let Some(client) = pry!(cap.make_cap(r)) {
                    let mut b = result.get().init_selected();
                    b.set_cap_type(cap.cap_type());
                    b.init_cap().set_as_capability(client.hook);
                    return Promise::ok(());
                }
            }
        }
        Promise::ok(())
    }
}
