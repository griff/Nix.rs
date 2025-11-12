use std::rc::Rc;
use std::time::UNIX_EPOCH;

use bstr::ByteSlice as _;

use camino::{Utf8Path, Utf8PathBuf};
use capnp::capability::{FromClientHook, Promise};
use capnp::traits::{FromPointerBuilder, HasTypeId, SetterInput};
use capnp_rpc::new_client;
use nixrs::profile::{Generation, Profile, ProfileRoots};
use nixrs::store_path::{HasStoreDir, StoreDir};
use tracing::warn;

use crate::capnp::nix_daemon_capnp::nix_daemon;
use crate::capnp::nixrs_capnp::{
    generation, generation_cap, generation_info, profile, store_path_store,
};
use crate::convert::ReadInto;
use crate::lookup::{LookupParams, ParamsCap};
use crate::nixrs::RemoteStorePath;

pub struct ProfileLookupParams<'p> {
    pub path: &'p Utf8Path,
}

impl LookupParams for ProfileLookupParams<'_> {
    type Params = profile::lookup_params::Owned;
    type Return = profile::Client;
}
impl SetterInput<profile::lookup_params::Owned> for ProfileLookupParams<'_> {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut b = profile::lookup_params::Builder::init_pointer(builder, 0);
        b.set_path(input.path.as_str().as_bytes());
        Ok(())
    }
}

#[derive(Clone)]
pub struct LocalProfiles<R> {
    root: Rc<Utf8PathBuf>,
    path_store_client: store_path_store::Client,
    profile_roots: R,
}

impl<R> LocalProfiles<R> {
    pub fn new(
        root: &Utf8Path,
        path_store_client: store_path_store::Client,
        profile_roots: R,
    ) -> Self {
        LocalProfiles {
            root: Rc::new(root.to_path_buf()),
            path_store_client,
            profile_roots,
        }
    }
}

impl<R> ParamsCap for LocalProfiles<R>
where
    R: ProfileRoots + Clone + 'static,
{
    fn cap_type(&self) -> u64 {
        profile::Client::TYPE_ID
    }

    fn add_ref(&self) -> Box<dyn ParamsCap> {
        Box::new(self.clone())
    }

    fn make_cap(
        &self,
        params: crate::capnp::lookup_capnp::matcher::Reader,
    ) -> capnp::Result<Option<capnp::capability::Client>> {
        let r: profile::lookup_params::Reader = params.get_params().get_as()?;
        let profile = match r.get_path()?.to_str() {
            Ok(path_s) => Utf8Path::new(path_s),
            Err(err) => {
                let s = r.get_path()?.to_str_lossy();
                warn!("Profile path {s} contained invalid UTF-8: {err}");
                return Ok(None);
            }
        };
        if !profile.starts_with(self.root.as_std_path()) {
            return Ok(None);
        }
        let client: profile::Client = new_client(ProfileImpl {
            store: self.path_store_client.clone(),
            profile: Rc::new(Profile::new(profile, self.profile_roots.clone())?),
        });
        Ok(Some(client.cast_to()))
    }
}

pub struct ProfileImpl<R> {
    store: store_path_store::Client,
    profile: Rc<Profile<R>>,
}
impl<R> Clone for ProfileImpl<R> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            profile: self.profile.clone(),
        }
    }
}

impl<R> ProfileImpl<R>
where
    R: ProfileRoots + 'static,
{
    async fn build_generation(
        &self,
        generation: &Generation<'_, R>,
        mut builder: generation::Builder<'_>,
    ) -> capnp::Result<()> {
        let client = new_client(GenerationCapImpl {
            profile: self.clone(),
            number: generation.number,
        });
        builder.set_cap(client);
        self.build_generation_info(generation, builder.init_info())
            .await
    }

    async fn build_generation_info(
        &self,
        generation: &Generation<'_, R>,
        mut builder: generation_info::Builder<'_>,
    ) -> capnp::Result<()> {
        let store_path = generation.store_path().await?;
        let remote_store_path = RemoteStorePath::from_store_path(store_path, &self.store)?;
        builder.set_store_path(&remote_store_path)?;
        builder.set_number(generation.number);
        let time = if let Ok(elapsed) = generation.creation_time.duration_since(UNIX_EPOCH) {
            elapsed.as_secs() as i64
        } else {
            0
        };
        builder.set_creation_time(time);
        Ok(())
    }
}

impl<R> profile::Server for ProfileImpl<R>
where
    R: ProfileRoots + 'static,
{
    fn current_generation(
        &mut self,
        _params: profile::CurrentGenerationParams,
        mut result: profile::CurrentGenerationResults,
    ) -> Promise<(), capnp::Error> {
        let me = self.clone();
        Promise::from_future(async move {
            if let Some(generation) = me.profile.current_generation().await? {
                me.build_generation(&generation, result.get().init_generation())
                    .await?;
            }
            Ok(())
        })
    }

    fn list_generations(
        &mut self,
        _params: profile::ListGenerationsParams,
        mut result: profile::ListGenerationsResults,
    ) -> Promise<(), capnp::Error> {
        let me = self.clone();
        Promise::from_future(async move {
            let list = me.profile.list_generations().await?;
            let mut bl = result.get().init_generations(list.len() as u32);
            for (index, generation) in me.profile.list_generations().await?.into_iter().enumerate()
            {
                me.build_generation(&generation, bl.reborrow().get(index as u32))
                    .await?;
            }
            Ok(())
        })
    }

    fn create_generation(
        &mut self,
        params: profile::CreateGenerationParams,
        mut result: profile::CreateGenerationResults,
    ) -> Promise<(), capnp::Error> {
        let me = self.clone();
        Promise::from_future(async move {
            let r = params.get()?;
            let mut req = me.store.lookup_request();
            req.get()
                .init_params()
                .set_by_store_path(r.get_store_path()?)?;
            let res = req.send().promise.await?;
            let store_path = r.get_store_path()?.read_into()?;
            let r = res.get()?;
            if !r.has_path() {
                return Err(capnp::Error::failed(
                    "Store (store_path) path does not exist".to_string(),
                ));
            }
            let remote_store_path = r.get_path()?.read_into()?;
            let generation = me.profile.create_generation(&store_path).await?;
            let mut b = result.get().init_generation();
            let client = new_client(GenerationCapImpl {
                profile: me.clone(),
                number: generation.number,
            });
            b.set_cap(client);
            let mut bi = b.init_info();
            bi.set_store_path(&remote_store_path)?;
            bi.set_number(generation.number);
            let time = if let Ok(elapsed) = generation.creation_time.duration_since(UNIX_EPOCH) {
                elapsed.as_secs() as i64
            } else {
                0
            };
            bi.set_creation_time(time);
            Ok(())
        })
    }
}

struct GenerationCapImpl<R> {
    profile: ProfileImpl<R>,
    number: u64,
}

impl<R> Clone for GenerationCapImpl<R> {
    fn clone(&self) -> Self {
        Self {
            profile: self.profile.clone(),
            number: self.number,
        }
    }
}

impl<R> generation_cap::Server for GenerationCapImpl<R>
where
    R: ProfileRoots + 'static,
{
    fn info(
        &mut self,
        _: generation_cap::InfoParams,
        mut result: generation_cap::InfoResults,
    ) -> Promise<(), capnp::Error> {
        let me = self.clone();
        Promise::from_future(async move {
            let generation = me.profile.profile.get_generation(me.number).await?;
            me.profile
                .build_generation_info(&generation, result.get().init_info())
                .await?;
            Ok(())
        })
    }

    fn switch(
        &mut self,
        _: generation_cap::SwitchParams,
        _: generation_cap::SwitchResults,
    ) -> Promise<(), capnp::Error> {
        let me = self.clone();
        Promise::from_future(async move {
            let generation = me.profile.profile.get_generation(me.number).await?;
            generation.switch().await?;
            Ok(())
        })
    }

    fn delete(
        &mut self,
        _: generation_cap::DeleteParams,
        _: generation_cap::DeleteResults,
    ) -> Promise<(), capnp::Error> {
        let me = self.clone();
        Promise::from_future(async move {
            let generation = me.profile.profile.get_generation(me.number).await?;
            generation.delete().await?;
            Ok(())
        })
    }
}

#[derive(Clone)]
pub struct DaemonProfileRoots {
    store_dir: StoreDir,
    client: nix_daemon::Client,
}

impl DaemonProfileRoots {
    pub fn new(nix_daemon_client: nix_daemon::Client) -> Self {
        Self::with_store_dir(nix_daemon_client, Default::default())
    }
    pub fn with_store_dir(nix_daemon_client: nix_daemon::Client, store_dir: StoreDir) -> Self {
        Self {
            client: nix_daemon_client,
            store_dir,
        }
    }
}

impl HasStoreDir for DaemonProfileRoots {
    fn store_dir(&self) -> &StoreDir {
        &self.store_dir
    }
}
impl ProfileRoots for DaemonProfileRoots {
    async fn make_gc_symlink(
        &self,
        link: &std::path::Path,
        target: &nixrs::store_path::StorePath,
    ) -> std::io::Result<()> {
        let mut req = self.client.add_perm_root_request();
        let mut b = req.get();
        let n = <[u8]>::from_os_str(link.as_os_str())
            .ok_or_else(|| std::io::Error::other(format!("link {link:?} is not valid UTF-8")))?;
        b.set_gc_root(n);
        b.set_path(target).map_err(std::io::Error::other)?;
        req.send().promise.await.map_err(std::io::Error::other)?;
        Ok(())
    }
}
