use std::pin::pin;
use std::time::{Duration, Instant};

use futures::StreamExt as _;
use nixrs::daemon::wire::types::Operation;
use nixrs::daemon::{DaemonStore as _, ProtocolVersion};
use nixrs::log::LogMessage;
use nixrs::pretty_prop_assert_eq;
use nixrs::store_path::{StorePath, StorePathSet};
use nixrs::test::daemon::mock::MockStore;
use proptest::prelude::*;
use proptest::sample::size_range;
use proptest::test_runner::TestCaseResult;
use tokio::time::timeout;
use tracing::error;

use crate::{ENV_NIX_IMPL, NixImpl as _, nix_protocol_range, prepare_mock, run_store_test};

#[test_log::test(test_strategy::proptest(
    async = "tokio",
    ProptestConfig::default(),
    max_shrink_iters = 30_000,
))]
async fn proptest_handshake(
    #[any(nix_protocol_range())] version: ProtocolVersion,
    #[any((size_range(0..100), #version))] mut op_logs: Vec<LogMessage>,
) -> TestCaseResult {
    let nix = &*ENV_NIX_IMPL;
    if nix.is_skipped("proptests::proptest_handshake") {
        return Ok(());
    }
    let mut mock = MockStore::builder();
    let store_path = "00000000000000000000000000000000-_".parse().unwrap();
    let mut op = mock.is_valid_path(&store_path, Ok(true));
    for log in op_logs.iter() {
        op = op.add_log(log.clone());
    }
    op.build();
    nix.prepare_op_logs(Operation::IsValidPath, &mut op_logs);
    let res = timeout(
        Duration::from_secs(60),
        run_store_test(nix, version, mock, |mut client, _| async move {
            {
                let ret = client.is_valid_path(&store_path);
                let mut r = pin!(ret);
                let actual_logs = r.by_ref().collect::<Vec<_>>().await;
                pretty_prop_assert_eq!(
                    actual_logs,
                    op_logs
                        .into_iter()
                        .map(|log| nix.collect_log(log))
                        .collect::<Vec<_>>()
                );
                r.await?;
            }
            Ok(client) as Result<_, TestCaseError>
        }),
    )
    .await;
    match res {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(err)) => {
            //error!("Test failed {}", err);
            Err(err)
        }
        Err(_) => {
            error!("Timeout waiting for test to complete");
            Err(TestCaseError::fail("Timeout waiting for test to complete"))
        }
    }
}

/*
proptest! {
    #[test]
    fn proptest_set_options(
        options in any::<ClientOptions>(),
    )
    {
        let now = Instant::now();
        let r = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        r.block_on(async {
            let mut mock = MockStore::builder();
            mock.set_options(&options, Ok(())).build();
            run_store_test(nix, mock, |mut client| async move {
                let res = client.set_options(&options).result().await;
                prop_assert!(res.is_ok(), "invalid result {:?}", res);
                Ok(client)
            }).await?;
            Ok(()) as Result<_, TestCaseError>
        })?;
        eprintln!("Completed test {}", now.elapsed().as_secs_f64());
    }
}
*/

#[test_log::test(test_strategy::proptest(async = "tokio", ProptestConfig::default(),))]
async fn proptest_is_valid_path(
    #[any(nix_protocol_range())] version: ProtocolVersion,
    #[any((size_range(0..100), #version))] mut op_logs: Vec<LogMessage>,
    #[any()] path: StorePath,
    #[any()] result: bool,
) -> TestCaseResult {
    let nix = &*ENV_NIX_IMPL;
    if nix.is_skipped("proptests::proptest_is_valid_path") {
        return Ok(());
    }
    let now = Instant::now();
    let mut mock = prepare_mock(nix);
    let mut op = mock.is_valid_path(&path, Ok(result));
    for log in op_logs.iter() {
        op = op.add_log(log.clone());
    }
    op.build();
    nix.prepare_op_logs(Operation::IsValidPath, &mut op_logs);
    run_store_test(nix, version, mock, |mut client, _| async move {
        {
            let ret = client.is_valid_path(&path);
            let mut r = pin!(ret);
            let actual_logs = r.by_ref().collect::<Vec<_>>().await;
            pretty_prop_assert_eq!(r.await?, result);
            pretty_prop_assert_eq!(
                actual_logs,
                op_logs
                    .into_iter()
                    .map(|log| nix.collect_log(log))
                    .collect::<Vec<_>>()
            );
        }
        Ok(client)
    })
    .await?;
    eprintln!("Completed test {}", now.elapsed().as_secs_f64());
    Ok(())
}

#[test_log::test(test_strategy::proptest(async = "tokio"))]
async fn proptest_query_valid_paths(
    #[any(nix_protocol_range())] version: ProtocolVersion,
    #[any()] paths: StorePathSet,
    #[any()] result: StorePathSet,
) -> TestCaseResult {
    let nix = &*ENV_NIX_IMPL;
    if nix.is_skipped("proptests::proptest_query_valid_paths") {
        return Ok(());
    }
    let now = Instant::now();
    let mut mock = prepare_mock(nix);
    mock.query_valid_paths(&paths, false, Ok(result.clone()))
        .build();
    run_store_test(nix, version, mock, |mut client, _| async move {
        let res = client.query_valid_paths(&paths, false).await;
        pretty_prop_assert_eq!(res.unwrap(), result);
        Ok(client)
    })
    .await?;
    eprintln!("Completed test {}", now.elapsed().as_secs_f64());
    Ok(())
}

// TODO: proptest query_valid_paths
// TODO: proptest query_path_info
// TODO: proptest nar_from_path

// TODO: proptest all messages
/*
#[test_log::test(test_strategy::proptest(
    async = "tokio",
    ProptestConfig::default(),
    max_shrink_iters = 30_000,
))]
async fn proptest_operations(
    #[any(#nix.protocol_range())]
    version: ProtocolVersion,
    #[any((#nix.handshake_logs_range(), #version))]
    handshake_logs: Vec<LogMessage>,
    #[any((size_range(0..10), MockOperationParams { version: #version, allow_options: false }))]
    ops: Vec<LogOperation>,
) -> TestCaseResult {
     if nix.is_skipped("proptests::proptest_operations") {
        return Ok(());
    }
    let mut mock = MockStore::builder();
    for op in ops.iter() {
        mock.add_operation(op.clone());
    }
    let handshake_logs = handshake_logs.into_iter().map(|log| nix.collect_log(log)).collect::<Vec<_>>();
    let op_types : Vec<_> = ops.iter().map(|o| o.operation.operation()).collect();
    info!(?op_types, "Running {} operations", ops.len());
    let res = timeout(Duration::from_secs(60),
        run_store_test(nix, version, mock, |mut client, actual_logs| async move {
            pretty_prop_assert_eq!(actual_logs, handshake_logs);
            for mut op in ops.into_iter() {
                op.logs = op.logs.into_iter().map(|log| nix.collect_log(log)).collect();
                nix.prepare_op_logs2(op.operation.operation(), &mut op.logs);
                op.check_operation(&mut client).await?;
            }
            Ok(client) as Result<_, TestCaseError>
        })
    ).await;
    match res {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(err)) => {
            //error!("Test failed {}", err);
            Err(err)
        }
        Err(_) => {
            error!("Timeout waiting for test to complete");
            Err(TestCaseError::fail("Timeout waiting for test to complete"))
        }
    }
}
*/

#[test_log::test(test_strategy::proptest(async = "tokio"))]
async fn proptest_op_logs(
    #[any(nix_protocol_range())] version: ProtocolVersion,
    #[any((size_range(0..100), #version))] mut op_logs: Vec<LogMessage>,
) -> TestCaseResult {
    let nix = &*ENV_NIX_IMPL;
    if nix.is_skipped("proptests::proptest_op_logs") {
        return Ok(());
    }
    let mut mock = MockStore::builder();
    let store_path = "00000000000000000000000000000000-_".parse().unwrap();
    let mut op = mock.is_valid_path(&store_path, Ok(true));
    for log in op_logs.iter() {
        op = op.add_log(log.clone());
    }
    op.build();
    nix.prepare_op_logs(Operation::IsValidPath, &mut op_logs);
    let res = timeout(
        Duration::from_secs(60),
        run_store_test(nix, version, mock, |mut client, _| async move {
            {
                let ret = client.is_valid_path(&store_path);
                let mut r = pin!(ret);
                let actual_logs = r.by_ref().collect::<Vec<_>>().await;
                pretty_prop_assert_eq!(
                    actual_logs,
                    op_logs
                        .into_iter()
                        .map(|log| nix.collect_log(log))
                        .collect::<Vec<_>>()
                );
                r.await?;
            }
            Ok(client) as Result<_, TestCaseError>
        }),
    )
    .await;
    match res {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(err)) => {
            //error!("Test failed {}", err);
            Err(err)
        }
        Err(_) => {
            error!("Timeout waiting for test to complete");
            Err(TestCaseError::fail("Timeout waiting for test to complete"))
        }
    }
}
