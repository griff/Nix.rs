use log::{error, debug};
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::get_protocol_minor;
use crate::legacy_local_store::{
    ServeCommand, SERVE_MAGIC_1, SERVE_MAGIC_2, SERVE_PROTOCOL_VERSION,
};
use crate::Error;
use crate::{BasicDerivation, BuildSettings, CheckSignaturesFlag, DerivedPath};
use crate::{RepairFlag, Store, StorePath, StorePathSet, StorePathWithOutputs};
use crate::{SubstituteFlag, ValidPathInfo};
use nixrs_util::hash;
use nixrs_util::io::{AsyncSink, AsyncSource};

pub async fn serve<S, R, W, BW>(
    mut source: R,
    mut out: W,
    mut store: S,
    mut build_log: BW,
    write_allowed: bool,
) -> Result<(), Error>
where
    S: Store,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    BW: AsyncWrite + Unpin + Send,
{
    let store_dir = store.store_dir();
    let magic = source.read_u64_le().await?;
    if magic != SERVE_MAGIC_1 {
        return Err(Error::LegacyProtocolServeMismatch(magic));
    }
    out.write_u64_le(SERVE_MAGIC_2).await?;
    out.write_u64_le(SERVE_PROTOCOL_VERSION).await?;
    out.flush().await?;
    let client_version = source.read_u64_le().await?;

    while let Ok(command) = source.read_enum::<ServeCommand>().await {
        debug!("Got command {}", command);
        match command {
            ServeCommand::CmdQueryValidPaths => {
                let lock = source.read_bool().await? && write_allowed;
                let substitute = source.read_bool().await?;
                let paths: StorePathSet = source.read_parsed_coll(&store_dir).await?;

                let maybe_substitute = if substitute && write_allowed {
                    SubstituteFlag::Substitute
                } else {
                    SubstituteFlag::NoSubstitute
                };
                let ret = store
                    .legacy_query_valid_paths(&paths, lock, maybe_substitute)
                    .await?;
                out.write_printed_coll(&store_dir, &ret).await?;
            }
            ServeCommand::CmdQueryPathInfos => {
                let paths: StorePathSet = source.read_parsed_coll(&store_dir).await?;
                // !!! Maybe we want a queryPathInfos?
                for i in paths {
                    match store.query_path_info(&i).await {
                        Ok(info) => {
                            out.write_printed(&store_dir, &info.path).await?;
                            if let Some(deriver) = info.deriver.as_ref() {
                                out.write_printed(&store_dir, deriver).await?;
                            } else {
                                out.write_str("").await?;
                            }
                            out.write_printed_coll(&store_dir, &info.references).await?;
                            // !!! Maybe we want compression?
                            out.write_u64_le(info.nar_size).await?; // download_size
                            out.write_u64_le(info.nar_size).await?;
                            if get_protocol_minor!(client_version) >= 4 {
                                let s = info.nar_hash.to_base32().to_string();
                                out.write_str(&s).await?;

                                if let Some(ca) = info.ca.as_ref() {
                                    let ca = ca.to_string();
                                    out.write_str(&ca).await?;
                                } else {
                                    out.write_str("").await?;
                                }

                                out.write_string_coll(&info.sigs).await?;
                            }
                        }
                        Err(Error::InvalidPath(_)) => (),
                        Err(err) => return Err(err),
                    }
                }
                out.write_str("").await?;
            }
            ServeCommand::CmdDumpStorePath => {
                let path = source.read_parsed(&store_dir).await?;
                store.nar_from_path(&path, &mut out).await?;
            }
            ServeCommand::CmdImportPaths => {
                if !write_allowed {
                    return Err(Error::WriteOnlyLegacyStore(command));
                }
                store.import_paths(&mut source).await?; // FIXME: should we support sig checking?
                out.write_u64_le(1).await?; // indicate success
            }
            ServeCommand::CmdExportPaths => {
                source.read_u64_le().await?; // obsolete
                let paths = source.read_parsed_coll(&store_dir).await?;
                store.export_paths(&paths, &mut out).await?;
            }
            ServeCommand::CmdBuildPaths => {
                if !write_allowed {
                    return Err(Error::WriteOnlyLegacyStore(command));
                }

                let paths: Vec<StorePathWithOutputs> = source.read_parsed_coll(&store_dir).await?;

                let mut settings = BuildSettings::default();
                // TODO: let verbosity = Error;
                settings.keep_log = false;
                settings.use_substitutes = false;
                settings.max_silent_time = source.read_seconds().await?;
                settings.build_timeout = source.read_seconds().await?;
                if get_protocol_minor!(client_version) >= 2 {
                    settings.max_log_size = source.read_u64_le().await?;
                }
                if get_protocol_minor!(client_version) >= 3 {
                    settings.build_repeat = source.read_u64_le().await?;
                    settings.enforce_determinism = source.read_bool().await?;
                    settings.run_diff_hook = true;
                }
                settings.print_repeated_builds = false;

                // TODO: MonitorFdHup monitor(in.fd);
                let drv_paths: Vec<DerivedPath> = paths.into_iter().map(|e| e.into()).collect();

                match store.build_paths(&drv_paths, &settings, &mut build_log).await {
                    Ok(_) => out.write_u64_le(0).await?,
                    Err(err) => {
                        assert!(err.exit_code() != 0);
                        out.write_u64_le(err.exit_code()).await?;
                        out.write_str(&err.to_string()).await?;
                    }
                }
            }
            ServeCommand::CmdBuildDerivation => {
                /* Used by hydra-queue-runner. */
                if !write_allowed {
                    return Err(Error::WriteOnlyLegacyStore(command));
                }

                let drv_path: StorePath = source.read_parsed(&store_dir).await?;
                let drv =
                    BasicDerivation::read_drv(&mut source, &store_dir, &drv_path.name_from_drv())
                        .await?;

                let mut settings = BuildSettings::default();
                // TODO: let verbosity = Error;
                settings.keep_log = false;
                settings.use_substitutes = false;
                settings.max_silent_time = source.read_seconds().await?;
                settings.build_timeout = source.read_seconds().await?;
                if get_protocol_minor!(client_version) >= 2 {
                    settings.max_log_size = source.read_u64_le().await?;
                }
                if get_protocol_minor!(client_version) >= 3 {
                    settings.build_repeat = source.read_u64_le().await?;
                    settings.enforce_determinism = source.read_bool().await?;
                    settings.run_diff_hook = true;
                }
                settings.print_repeated_builds = false;

                // TODO: MonitorFdHup monitor(in.fd);
                let status = store.build_derivation(&drv_path, &drv, &settings, &mut build_log).await?;
                out.write_enum(status.status).await?;
                out.write_str(&status.error_msg).await?;
                if !status.success() {
                    error!("Build failed {:?}: {}", status.status, status.error_msg);
                }

                if get_protocol_minor!(client_version) >= 3 {
                    out.write_u64_le(status.times_built).await?;
                    out.write_bool(status.is_non_deterministic).await?;
                    out.write_time(status.start_time).await?;
                    out.write_time(status.stop_time).await?;
                }
                if get_protocol_minor!(client_version) >= 6 {
                    out.write_usize(status.built_outputs.len()).await?;
                    for (key, val) in status.built_outputs {
                        out.write_str(&key.to_string()).await?;
                        out.write_str(&val.to_json_string()?).await?;
                    }
                }
            }
            ServeCommand::CmdQueryClosure => {
                let include_outputs = source.read_bool().await?;
                let paths: StorePathSet = source.read_parsed_coll(&store_dir).await?;
                let closure = store.query_closure(&paths, include_outputs).await?;
                out.write_printed_coll(&store_dir, &closure).await?
            }
            ServeCommand::CmdAddToStoreNar => {
                if !write_allowed {
                    return Err(Error::WriteOnlyLegacyStore(command));
                }

                let path = source.read_parsed(&store_dir).await?;
                let deriver = source.read_string().await?;
                let deriver = if deriver != "" {
                    Some(store_dir.parse_path(&deriver)?)
                } else {
                    None
                };
                let nar_hash = source.read_string().await?;
                let nar_hash = hash::Hash::parse_any(&nar_hash, Some(hash::Algorithm::SHA256))?;
                let references = source.read_parsed_coll(&store_dir).await?;
                let registration_time = source.read_time().await?;
                let nar_size = source.read_u64_le().await?;
                let ultimate = source.read_bool().await?;
                let sigs = source.read_string_coll().await?;
                let ca_s = source.read_string().await?;
                let ca = if ca_s != "" {
                    Some(ca_s.parse()?)
                } else {
                    None
                };

                if nar_size == 0 {
                    return Err(Error::Misc(
                        "narInfo is too old and missing the narSize field".into(),
                    ));
                }
                let mut sized_source = tokio::io::AsyncReadExt::take(&mut source, nar_size);
                let info = ValidPathInfo {
                    path,
                    deriver,
                    nar_hash,
                    references,
                    nar_size,
                    ultimate,
                    sigs,
                    ca,
                    registration_time,
                };
                store
                    .add_to_store(
                        &info,
                        &mut sized_source,
                        RepairFlag::NoRepair,
                        CheckSignaturesFlag::NoCheckSigs,
                    )
                    .await?;

                // consume all the data that has been sent before continuing.
                sized_source.drain_all().await?;

                out.write_u64_le(1).await?; // indicate success
            }
            ServeCommand::Unknown(cmd) => {
                return Err(Error::Misc(format!("unknown serve command {}", cmd)));
            }
        }
        debug!("Flushing!");
        out.flush().await?;
        debug!("Loop");
    }
    debug!("Serve done");

    Ok(())
}
