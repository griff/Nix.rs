use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use futures::future::Ready;
use futures::{Future, FutureExt};
use log::{debug, error, info};
use thrussh::server::Config;
use thrussh::{
    server::{self, Handle},
    ChannelId, ChannelOpenFailure, CryptoVec,
};
use thrussh_keys::key::{KeyPair, PublicKey};
use thrussh_keys::{decode_secret_key, key, parse_public_key_base64, PublicKeyBase64};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::io::{ChannelRead, DataWrite, ExtendedDataWrite};
use crate::StoreProvider;

#[derive(Debug)]
pub struct ServerConfig<S> {
    config: Config,
    user_keys: HashMap<String, bool>,
    store_provider: S,
}

async fn load_file(path: impl AsRef<Path>) -> io::Result<String> {
    let mut f = File::open(path).await?;
    let mut s = String::new();
    f.read_to_string(&mut s).await?;
    Ok(s)
}

async fn load_secret_key(
    path: impl AsRef<Path>,
    password: Option<&str>,
) -> Result<KeyPair, thrussh_keys::Error> {
    let secret = load_file(path).await?;
    decode_secret_key(&secret, password)
}

async fn load_public_key(path: impl AsRef<Path>) -> Result<PublicKey, thrussh_keys::Error> {
    let pubkey = load_file(path).await?;

    let mut split = pubkey.split_whitespace();
    match (split.next(), split.next()) {
        (Some(_), Some(key)) => parse_public_key_base64(key),
        (Some(key), None) => parse_public_key_base64(key),
        _ => Err(thrussh_keys::Error::CouldNotReadKey.into()),
    }
}

impl<S> ServerConfig<S> {
    pub fn with_store(store_provider: S) -> ServerConfig<S> {
        ServerConfig {
            config: Default::default(),
            user_keys: Default::default(),
            store_provider,
        }
    }

    pub fn add_host_key(&mut self, key: KeyPair) -> &mut Self {
        self.config.keys.push(key);
        self
    }

    pub async fn load_host_key(
        &mut self,
        path: impl AsRef<Path>,
    ) -> Result<(), thrussh_keys::Error> {
        match load_secret_key(path, None).await {
            Ok(key) => {
                self.add_host_key(key);
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    pub async fn load_host_keys(&mut self, config_dir: impl AsRef<Path>) {
        let config_dir = config_dir.as_ref();
        self.load_host_key(config_dir.join("ssh_host_ed25519_key"))
            .await
            .ok();
        self.load_host_key(config_dir.join("ssh_host_rsa_key"))
            .await
            .ok();
    }

    pub fn add_user_key(&mut self, key: PublicKey, write_allowed: bool) -> &mut Self {
        self.user_keys
            .insert(key.public_key_base64(), write_allowed);
        self
    }

    pub async fn load_user_key(
        &mut self,
        path: impl AsRef<Path>,
        write_allowed: bool,
    ) -> Result<(), thrussh_keys::Error> {
        match load_public_key(path).await {
            Ok(key) => {
                self.add_user_key(key, write_allowed);
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    pub async fn load_user_keys(&mut self, config_dir: impl AsRef<Path>) {
        let config_dir = config_dir.as_ref();
        self.load_user_key(config_dir.join("id_ed25519.pub"), true)
            .await
            .ok();
        self.load_user_key(config_dir.join("id_rsa.pub"), true)
            .await
            .ok();
    }

    pub async fn load_default_keys(&mut self, config_dir: impl AsRef<Path>) {
        let config_dir = config_dir.as_ref();
        self.load_host_keys(&config_dir).await;
        self.load_user_keys(&config_dir).await;
    }
}

pub struct Server<S> {
    state: ServerState<S>,
    serve_rx: mpsc::UnboundedReceiver<ChannelMsg>,
}

struct ChannelMsg {
    channel: ChannelId,
    handle: Handle,
    source: ChannelRead,
    write_allowed: bool,
    reply: oneshot::Sender<JoinHandle<Result<(), anyhow::Error>>>,
}

#[derive(Clone)]
pub struct ServerState<S> {
    config: Arc<Config>,
    user_keys: Arc<HashMap<String, bool>>,
    serve_tx: mpsc::UnboundedSender<ChannelMsg>,
    store_provider: S,
    shutdown: CancellationToken,
}

impl<S> ServerState<S> {
    pub fn shutdown(&self) {
        self.shutdown.cancel()
    }
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.is_cancelled()
    }
}

impl<S: Clone> ServerState<S> {
    pub fn store_provider(&self) -> S {
        self.store_provider.clone()
    }
}

impl<S: Clone> Server<S> {
    pub fn store_provider(&self) -> S {
        self.state.store_provider.clone()
    }

    pub fn state(&self) -> ServerState<S> {
        self.state.clone()
    }
}

impl<S> Server<S>
where
    S: StoreProvider + Clone + Send + 'static,
{
    pub fn with_config(config: ServerConfig<S>) -> io::Result<Server<S>> {
        let (serve_tx, serve_rx) = mpsc::unbounded_channel();
        let shutdown = CancellationToken::new();
        let state = ServerState {
            serve_tx,
            shutdown,
            user_keys: Arc::new(config.user_keys),
            config: Arc::new(config.config),
            store_provider: config.store_provider,
        };
        Ok(Server { state, serve_rx })
    }

    pub async fn run(self, addr: &str) -> io::Result<()> {
        let local = tokio::task::LocalSet::new();
        let store_provider = self.state.store_provider.clone();
        let cancel = self.state.shutdown.clone();
        let server_cancel = self.state.shutdown.clone();
        local.run_until(async move {
            let mut serve_rx = self.serve_rx;
            tokio::task::spawn_local(async move {
                loop {
                    select! {
                        msg = serve_rx.recv() => {
                            match msg {
                                Some(ChannelMsg { channel, handle, source, write_allowed, reply }) => {
                                    let cancel = cancel.clone();
                                    let store_provider = store_provider.clone();
                                    let join = tokio::task::spawn_local(async move {
                                        let stderr = ExtendedDataWrite::new(channel, 1, handle.clone());
                                        let out = DataWrite::new(channel, handle);
                                        let store = store_provider.get_store(stderr.clone()).await?;
                                        select! {
                                            res = nixrs_store::legacy_worker::server::run(source, out, store, stderr, write_allowed) => {
                                                match res {
                                                    Ok(_) => {},
                                                    Err(err) => {
                                                        tracing::error!("Error in serve {:?}", err);
                                                        return Err(err.into());
                                                    }
                                                }
                                            }
                                            _ = cancel.cancelled() => {
                                                info!("Shutting down channel {:?}!", channel);
                                                Err(io::Error::new(io::ErrorKind::BrokenPipe, "Shutting down"))?;
                                            }
                                        }
                                        Ok(())
                                    });
                                    reply.send(join).unwrap_or_default();
                                },
                                None => break,
                            }
                        }
                        _ = cancel.cancelled() => {
                            info!("Shutting down channel handler!");
                            break
                        }
                    }

                }
            });
            let config = self.state.config.clone();
            info!("Running SSH on {}", addr);
            select! {
                res = thrussh::server::run(config, addr, self.state) => {
                    info!("SSH server completed");
                    res
                }
                _ = server_cancel.cancelled() => {
                    info!("Shutting down SSH server");
                    Ok(())
                }
            }
        }).await
    }
}

impl<S> server::Server for ServerState<S> {
    type Handler = ServerHandler;

    fn new(&mut self, _peer_addr: Option<std::net::SocketAddr>) -> Self::Handler {
        ServerHandler {
            channels: HashMap::new(),
            user_keys: self.user_keys.clone(),
            serve_tx: self.serve_tx.clone(),
            auth_user: None,
        }
    }
}

pub struct ServerHandler {
    channels: HashMap<ChannelId, ServerChannel>,
    user_keys: Arc<HashMap<String, bool>>,
    serve_tx: mpsc::UnboundedSender<ChannelMsg>,
    auth_user: Option<(String, bool)>,
}

impl server::Handler for ServerHandler {
    type Error = anyhow::Error;

    type FutureAuth = Ready<Result<(Self, server::Auth), anyhow::Error>>;

    type FutureUnit =
        Pin<Box<dyn Future<Output = Result<(Self, server::Session), anyhow::Error>> + Send>>;

    type FutureBool = Ready<Result<(Self, server::Session, bool), anyhow::Error>>;

    fn finished_auth(self, auth: server::Auth) -> Self::FutureAuth {
        futures::future::ready(Ok((self, auth)))
    }

    fn finished_bool(self, b: bool, session: server::Session) -> Self::FutureBool {
        futures::future::ready(Ok((self, session, b)))
    }

    fn finished(self, session: server::Session) -> Self::FutureUnit {
        Box::pin(futures::future::ready(Ok((self, session))))
    }

    fn auth_publickey(mut self, user: &str, public_key: &key::PublicKey) -> Self::FutureAuth {
        debug!("Auth key {} {}", user, public_key.public_key_base64());
        let key = public_key.public_key_base64();
        if let Some(write_allowed) = self.user_keys.get(&key) {
            self.auth_user = Some((key, *write_allowed));
            self.finished_auth(server::Auth::Accept)
        } else {
            self.finished_auth(server::Auth::Reject)
        }
    }

    fn channel_close(mut self, channel: ChannelId, session: server::Session) -> Self::FutureUnit {
        debug!("Channel close {:?}", channel);
        self.channels.remove(&channel);
        self.finished(session)
    }

    fn channel_eof(mut self, channel: ChannelId, session: server::Session) -> Self::FutureUnit {
        if let Some(ch) = self.channels.get_mut(&channel) {
            debug!("Got EOF for {:?}", channel);
            ch.sender.send(Vec::new()).unwrap_or_default();
            if let Some(join) = ch.serve.take() {
                return Box::pin(join.map(move |res| match res {
                    Ok(Ok(_)) => Ok((self, session)),
                    Ok(Err(err)) => Err(err),
                    Err(err) => Err(err.into()),
                }));
            }
        }
        self.finished(session)
    }

    fn channel_open_session(
        mut self,
        channel: ChannelId,
        session: server::Session,
    ) -> Self::FutureUnit {
        let (stdin, sender) = ChannelRead::new();
        self.channels.insert(
            channel,
            ServerChannel {
                sender,
                stdin: Some(stdin),
                serve: None,
            },
        );
        self.finished(session)
    }

    fn channel_open_x11(
        self,
        channel: ChannelId,
        _originator_address: &str,
        _originator_port: u32,
        mut session: server::Session,
    ) -> Self::FutureUnit {
        session.channel_open_failure(
            channel,
            ChannelOpenFailure::UnknownChannelType,
            "Invalid channel type",
            "en",
        );
        self.finished(session)
    }

    fn channel_open_direct_tcpip(
        self,
        channel: ChannelId,
        _host_to_connect: &str,
        _port_to_connect: u32,
        _originator_address: &str,
        _originator_port: u32,
        mut session: server::Session,
    ) -> Self::FutureUnit {
        session.channel_open_failure(
            channel,
            ChannelOpenFailure::UnknownChannelType,
            "Invalid channel type",
            "en",
        );
        self.finished(session)
    }

    fn data(self, channel: ChannelId, data: &[u8], session: server::Session) -> Self::FutureUnit {
        if let Some(ch) = self.channels.get(&channel) {
            ch.sender.send(data.into()).unwrap_or_default();
        }
        self.finished(session)
    }

    fn pty_request(
        self,
        channel: ChannelId,
        _term: &str,
        _col_width: u32,
        _row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _modes: &[(thrussh::Pty, u32)],
        mut session: server::Session,
    ) -> Self::FutureUnit {
        session.channel_failure(channel);
        self.finished(session)
    }

    fn x11_request(
        self,
        channel: ChannelId,
        _single_connection: bool,
        _x11_auth_protocol: &str,
        _x11_auth_cookie: &str,
        _x11_screen_number: u32,
        mut session: server::Session,
    ) -> Self::FutureUnit {
        session.channel_failure(channel);
        self.finished(session)
    }

    fn env_request(
        self,
        channel: ChannelId,
        _variable_name: &str,
        _variable_value: &str,
        mut session: server::Session,
    ) -> Self::FutureUnit {
        session.channel_failure(channel);
        self.finished(session)
    }

    fn shell_request(self, channel: ChannelId, mut session: server::Session) -> Self::FutureUnit {
        session.channel_failure(channel);
        self.finished(session)
    }

    fn exec_request(
        mut self,
        channel: ChannelId,
        data: &[u8],
        mut session: server::Session,
    ) -> Self::FutureUnit {
        let mut handle = session.handle();
        let mut write_allowed = match data {
            b"nix-store --serve --write" => true,
            b"nix-store --serve" => false,
            _ => {
                let err_txt = "invalid command".to_string();
                error!("{}", err_txt);
                session.extended_data(channel, 1, CryptoVec::from(err_txt));
                session.exit_status_request(channel, 1);
                session.close(channel);
                return self.finished(session);
            }
        };
        if let Some((_, user_write_allowed)) = self.auth_user.as_ref() {
            write_allowed = write_allowed && *user_write_allowed;
        }
        if let Some(ch) = self.channels.get_mut(&channel) {
            if let Some(source) = ch.stdin.take() {
                let (reply, reply_rx) = oneshot::channel();
                self.serve_tx
                    .send(ChannelMsg {
                        channel,
                        handle: handle.clone(),
                        source,
                        write_allowed,
                        reply,
                    })
                    .unwrap_or_default();
                let join = tokio::spawn(async move {
                    match reply_rx.await {
                        Ok(join) => match join.await {
                            Ok(Ok(_)) => Ok(()),
                            Ok(Err(err)) => {
                                let err_txt = format!("Exec failed {:?}", err);
                                error!("{}", err_txt);
                                handle
                                    .extended_data(channel, 1, CryptoVec::from(err_txt))
                                    .await
                                    .unwrap_or_default();
                                handle
                                    .exit_status_request(channel, 1)
                                    .await
                                    .unwrap_or_default();
                                handle.close(channel).await.unwrap_or_default();
                                Err(err)
                            }
                            Err(err) => {
                                let err_txt = format!("Exec join failed {:?}", err);
                                error!("{}", err_txt);
                                handle
                                    .extended_data(channel, 1, CryptoVec::from(err_txt))
                                    .await
                                    .unwrap_or_default();
                                handle
                                    .exit_status_request(channel, 1)
                                    .await
                                    .unwrap_or_default();
                                handle.close(channel).await.unwrap_or_default();
                                Err(err.into())
                            }
                        },
                        Err(err) => {
                            let err_txt = format!("Could not get join handle {:?}", err);
                            error!("{}", err_txt);
                            handle
                                .extended_data(channel, 1, CryptoVec::from(err_txt))
                                .await
                                .unwrap_or_default();
                            handle
                                .exit_status_request(channel, 1)
                                .await
                                .unwrap_or_default();
                            handle.close(channel).await.unwrap_or_default();
                            Err(err.into())
                        }
                    }
                });
                ch.serve = Some(join);
                /*
                let join = tokio::task::spawn_local(async move {
                    let stderr = ExtendedDataWrite::new(channel, 1, handle.clone());
                    let out = DataWrite::new(channel, handle);
                    let store = manager.get_store(stderr).await;
                    nixrs_store::nix_store::serve(source, out, store, true).await?;
                    Ok(())
                });
                ch.serve = Some(join);
                 */
            }
        }
        self.finished(session)
    }

    fn subsystem_request(
        self,
        channel: ChannelId,
        _name: &str,
        mut session: server::Session,
    ) -> Self::FutureUnit {
        session.channel_failure(channel);
        self.finished(session)
    }
}

struct ServerChannel {
    sender: mpsc::UnboundedSender<Vec<u8>>,
    stdin: Option<ChannelRead>,
    serve: Option<JoinHandle<Result<(), anyhow::Error>>>,
}
