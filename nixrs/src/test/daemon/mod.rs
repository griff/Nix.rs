pub mod de;
#[cfg(feature = "daemon")]
mod mock;
pub mod ser;

#[cfg(feature = "daemon")]
pub use crate::daemon::wire::types::Operation;
#[cfg(feature = "daemon")]
pub use mock::{
    Builder, ChannelReporter, LogBuild, LogBuilder, LogOperation, LogResult, MockOperation,
    MockReporter, MockRequest, MockResponse, MockStore, ReporterError, check_logs,
};
