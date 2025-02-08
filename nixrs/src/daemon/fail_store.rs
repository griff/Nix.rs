use super::{DaemonResultExt as _, DaemonStore, HandshakeDaemonStore};

#[derive(Debug)]
pub struct FailStore;

impl HandshakeDaemonStore for FailStore {
    type Store = Self;

    fn handshake(self) -> impl super::LoggerResult<Self::Store, super::DaemonError> {
        Ok(self)
    }
}

impl DaemonStore for FailStore {
    fn trust_level(&self) -> super::TrustLevel {
        todo!()
    }

    fn set_options<'a>(
        &'a mut self,
        _options: &'a super::ClientOptions,
    ) -> impl super::LoggerResult<(), super::DaemonError> + 'a {
        Err(super::DaemonErrorKind::UnimplementedOperation(
            super::wire::types::Operation::SetOptions,
        ))
        .with_operation(super::wire::types::Operation::SetOptions)
    }

    fn is_valid_path<'a>(
        &'a mut self,
        _path: &'a crate::store_path::StorePath,
    ) -> impl super::LoggerResult<bool, super::DaemonError> + 'a {
        Err(super::DaemonErrorKind::UnimplementedOperation(
            super::wire::types::Operation::IsValidPath,
        ))
        .with_operation(super::wire::types::Operation::IsValidPath)
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        _paths: &'a crate::store_path::StorePathSet,
        _substitute: bool,
    ) -> impl super::LoggerResult<crate::store_path::StorePathSet, super::DaemonError> + 'a {
        Err(super::DaemonErrorKind::UnimplementedOperation(
            super::wire::types::Operation::QueryValidPaths,
        ))
        .with_operation(super::wire::types::Operation::QueryValidPaths)
    }

    fn query_path_info<'a>(
        &'a mut self,
        _path: &'a crate::store_path::StorePath,
    ) -> impl super::LoggerResult<Option<super::UnkeyedValidPathInfo>, super::DaemonError> + 'a
    {
        Err(super::DaemonErrorKind::UnimplementedOperation(
            super::wire::types::Operation::QueryPathInfo,
        ))
        .with_operation(super::wire::types::Operation::QueryPathInfo)
    }

    fn nar_from_path<'a, 'p, 'r, NW>(
        &'a mut self,
        _path: &'p crate::store_path::StorePath,
        _sink: NW,
    ) -> impl super::LoggerResult<(), super::DaemonError> + 'r
    where
        NW: tokio::io::AsyncWrite + Unpin + Send + 'r,
        'a: 'r,
        'p: 'r,
    {
        Err(super::DaemonErrorKind::UnimplementedOperation(
            super::wire::types::Operation::NarFromPath,
        ))
        .with_operation(super::wire::types::Operation::NarFromPath)
    }
}
