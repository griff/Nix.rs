mod async_sink;
mod async_source;
mod collection_read;
mod collection_size;
mod drain;
mod map_printed_state;
mod offset_reader;
mod read_exact;
mod read_int;
mod read_padding;
mod read_parsed;
mod read_parsed_coll;
mod read_string;
mod read_string_coll;
mod state_parse;
mod state_print;
mod write_all;
mod write_int;
mod write_owned_string_coll;
mod write_str;
mod write_string;
mod write_string_coll;

pub use async_sink::AsyncSink;
pub use async_source::AsyncSource;
pub use collection_read::CollectionRead;
pub use collection_size::CollectionSize;
pub use offset_reader::OffsetReader;
pub use state_parse::StateParse;
pub use state_print::StatePrint;

pub(crate) const STATIC_PADDING: &[u8] = &[0u8; 8];

pub fn calc_padding(size: u64) -> u8 {
    if size % 8 > 0 {
        8 - (size % 8) as u8
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::num::ParseIntError;
    use std::time::{Duration, SystemTime};

    use pretty_assertions::assert_eq;

    use crate::{flag_enum, string_set, StringSet};

    use super::*;

    flag_enum! {
        #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
        pub enum RepairFlag {
            NoRepair = false,
            Repair = true,
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum WrapError {
        #[error("I/O error {0}")]
        IO(#[from] std::io::Error),
        #[error("parse error {0}")]
        Parse(#[from] ParseIntError),
    }

    impl StateParse<u64> for u64 {
        type Err = WrapError;

        fn parse(&self, s: &str) -> Result<u64, Self::Err> {
            Ok(s.parse::<u64>()? + *self)
        }
    }

    impl StatePrint<u64> for u64 {
        fn print(&self, item: &u64) -> String {
            format!("{}", *item - *self)
        }
    }

    #[tokio::test]
    async fn test_write_usize() {
        let mut buf = Vec::new();
        buf.write_usize(44).await.unwrap();
        assert_eq!(buf.len(), 8);
        assert_eq!((&buf[..]).read_usize().await.unwrap(), 44);
    }

    #[tokio::test]
    async fn test_write_bool() {
        let mut buf = Vec::new();
        buf.write_bool(true).await.unwrap();
        assert_eq!(buf.len(), 8);
        assert_eq!((&buf[..]).read_bool().await.unwrap(), true);
    }

    #[tokio::test]
    async fn test_write_bool_false() {
        let mut buf = Vec::new();
        buf.write_bool(false).await.unwrap();
        assert_eq!((&buf[..]).read_bool().await.unwrap(), false);
        assert_eq!(buf.len(), 8);
    }

    #[tokio::test]
    async fn test_write_bool_trueish() {
        let mut buf = Vec::new();
        buf.write_usize(12).await.unwrap();
        assert_eq!((&buf[..]).read_bool().await.unwrap(), true);
    }

    #[tokio::test]
    async fn test_write_flag() {
        let mut buf = Vec::new();
        buf.write_flag(RepairFlag::NoRepair).await.unwrap();
        assert_eq!(
            (&buf[..]).read_flag::<RepairFlag>().await.unwrap(),
            RepairFlag::NoRepair
        );
        assert_eq!(buf.len(), 8);
    }

    #[tokio::test]
    async fn test_write_flag2() {
        let mut buf = Vec::new();
        buf.write_flag(RepairFlag::Repair).await.unwrap();
        assert_eq!(
            (&buf[..]).read_flag::<RepairFlag>().await.unwrap(),
            RepairFlag::Repair
        );
        assert_eq!(buf.len(), 8);
    }

    #[tokio::test]
    async fn test_write_seconds() {
        let mut buf = Vec::new();
        buf.write_seconds(Duration::from_secs(666)).await.unwrap();
        assert_eq!(
            (&buf[..]).read_seconds().await.unwrap(),
            Duration::from_secs(666)
        );
        assert_eq!(buf.len(), 8);
    }

    #[tokio::test]
    async fn test_write_seconds2() {
        let mut buf = Vec::new();
        buf.write_seconds(Duration::from_secs(1621144078))
            .await
            .unwrap();
        assert_eq!(
            (&buf[..]).read_seconds().await.unwrap(),
            Duration::from_secs(1621144078)
        );
        assert_eq!(buf.len(), 8);
    }

    #[tokio::test]
    async fn test_write_time_epoch() {
        let mut buf = Vec::new();
        buf.write_time(SystemTime::UNIX_EPOCH).await.unwrap();
        assert_eq!(
            (&buf[..]).read_time().await.unwrap(),
            SystemTime::UNIX_EPOCH
        );
        assert_eq!(buf.len(), 8);
    }

    #[tokio::test]
    async fn test_write_time_now() {
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        let time_s = Duration::from_secs(time.as_secs());
        let now = SystemTime::UNIX_EPOCH + time_s;
        let mut buf = Vec::new();
        buf.write_time(now).await.unwrap();
        assert_eq!((&buf[..]).read_time().await.unwrap(), now);
        assert_eq!(buf.len(), 8);
    }

    #[tokio::test]
    async fn test_write_string0() {
        let mut buf = Vec::new();
        buf.write_str("").await.unwrap();
        assert_eq!((&buf[..]).read_string().await.unwrap(), "");
        assert_eq!(buf.len(), 8);
    }

    #[tokio::test]
    async fn test_write_string1() {
        let mut buf = Vec::new();
        buf.write_str(")").await.unwrap();
        assert_eq!((&buf[..]).read_string().await.unwrap(), ")");
        assert_eq!(buf.len(), 16);
    }

    #[tokio::test]
    async fn test_write_string2() {
        let mut buf = Vec::new();
        buf.write_str("it").await.unwrap();
        assert_eq!((&buf[..]).read_string().await.unwrap(), "it");
        assert_eq!(buf.len(), 16);
    }

    #[tokio::test]
    async fn test_write_string3() {
        let mut buf = Vec::new();
        buf.write_str("tea").await.unwrap();
        assert_eq!((&buf[..]).read_string().await.unwrap(), "tea");
        assert_eq!(buf.len(), 16);
    }

    #[tokio::test]
    async fn test_write_string4() {
        let mut buf = Vec::new();
        buf.write_str("were").await.unwrap();
        assert_eq!((&buf[..]).read_string().await.unwrap(), "were");
        assert_eq!(buf.len(), 16);
    }

    #[tokio::test]
    async fn test_write_string5() {
        let mut buf = Vec::new();
        buf.write_str("where").await.unwrap();
        assert_eq!((&buf[..]).read_string().await.unwrap(), "where");
        assert_eq!(buf.len(), 16);
    }

    #[tokio::test]
    async fn test_write_string6() {
        let mut buf = Vec::new();
        buf.write_str("unwrap").await.unwrap();
        assert_eq!((&buf[..]).read_string().await.unwrap(), "unwrap");
        assert_eq!(buf.len(), 16);
    }

    #[tokio::test]
    async fn test_write_string7() {
        let mut buf = Vec::new();
        buf.write_str("where's").await.unwrap();
        assert_eq!((&buf[..]).read_string().await.unwrap(), "where's");
        assert_eq!(buf.len(), 16);
    }

    #[tokio::test]
    async fn test_write_string8() {
        let mut buf = Vec::new();
        buf.write_str("read_tea").await.unwrap();
        assert_eq!((&buf[..]).read_string().await.unwrap(), "read_tea");
        assert_eq!(buf.len(), 16);
    }

    #[tokio::test]
    async fn test_write_string9() {
        let mut buf = Vec::new();
        buf.write_str("read_tess").await.unwrap();
        assert_eq!((&buf[..]).read_string().await.unwrap(), "read_tess");
        assert_eq!(buf.len(), 24);
    }

    #[tokio::test]
    async fn test_write_strings0() {
        let mut buf = Vec::new();
        buf.write_string_coll(&vec![]).await.unwrap();
        let read: Vec<String> = (&buf[..]).read_string_coll().await.unwrap();
        assert_eq!(read, Vec::new() as Vec<String>);
        assert_eq!(buf.len(), 8);
    }

    #[tokio::test]
    async fn test_write_strings3() {
        let mut buf = Vec::new();
        buf.write_string_coll(&vec![
            "first".to_string(),
            "second".to_string(),
            "third".to_string(),
        ])
        .await
        .unwrap();
        let read: Vec<String> = (&buf[..]).read_string_coll().await.unwrap();
        assert_eq!(read, vec!["first", "second", "third"]);
        assert_eq!(buf.len(), 56);
    }

    #[tokio::test]
    async fn test_write_string_set() {
        let mut buf = Vec::new();
        buf.write_string_coll(&string_set!["first", "second", "third"])
            .await
            .unwrap();
        let read: StringSet = (&buf[..]).read_string_coll().await.unwrap();
        assert_eq!(read, string_set!["first", "second", "third"]);
        assert_eq!(buf.len(), 56);
    }

    #[tokio::test]
    async fn test_write_printed() {
        let mut buf = Vec::new();
        buf.write_printed(&(45 as u64), &(195 as u64))
            .await
            .unwrap();
        let read: u64 = (&buf[..]).read_parsed(&(45 as u64)).await.unwrap();
        assert_eq!(read, 195);
        let read: u64 = (&buf[..]).read_parsed(&(0 as u64)).await.unwrap();
        assert_eq!(read, 150);
        assert_eq!(buf.len(), 16);
    }

    #[tokio::test]
    async fn test_write_printed_coll() {
        let mut buf = Vec::new();
        let mut set: HashSet<u64> = HashSet::new();
        set.insert(195);
        set.insert(290);
        buf.write_printed_coll(&(45 as u64), &set).await.unwrap();
        let read: HashSet<u64> = (&buf[..]).read_parsed_coll(&(45 as u64)).await.unwrap();
        assert_eq!(read, set);
        let mut set2: HashSet<u64> = HashSet::new();
        set2.insert(150);
        set2.insert(245);
        let read: HashSet<u64> = (&buf[..]).read_parsed_coll(&(0 as u64)).await.unwrap();
        assert_eq!(read, set2);
        assert_eq!(buf.len(), 40);
    }
}
