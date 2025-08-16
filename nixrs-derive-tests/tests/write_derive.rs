use std::fmt;

use nixrs::{
    daemon::ser::{
        NixWrite as _,
        mock::{Builder, Error},
    },
    store_path::{StoreDir, StoreDirDisplay},
};
use nixrs_derive::NixSerialize;
use num_enum::IntoPrimitive;

#[derive(Debug, PartialEq, Eq, NixSerialize)]
pub struct UnitTest;

#[derive(Debug, PartialEq, Eq, NixSerialize)]
pub struct EmptyTupleTest();

#[derive(Debug, PartialEq, Eq, NixSerialize)]
pub struct StructTest {
    first: u64,
    second: String,
}

#[derive(Debug, PartialEq, Eq, NixSerialize)]
pub struct TupleTest(u64, String);

#[derive(Debug, PartialEq, Eq, NixSerialize)]
pub struct StructVersionTest {
    test: u64,
    #[nix(version = "20..")]
    hello: String,
}

fn default_test() -> StructVersionTest {
    StructVersionTest {
        test: 89,
        hello: String::from("klomp"),
    }
}

#[derive(Debug, PartialEq, Eq, NixSerialize)]
pub struct TupleVersionTest(u64, #[nix(version = "25..")] String);

#[derive(Debug, PartialEq, Eq, NixSerialize)]
pub struct TupleVersionDefaultTest(u64, #[nix(version = "..25")] StructVersionTest);

#[tokio::test]
async fn write_unit() {
    let mut mock = Builder::new().build();
    mock.write_value(&UnitTest).await.unwrap();
}

#[tokio::test]
async fn write_empty_tuple() {
    let mut mock = Builder::new().build();
    mock.write_value(&EmptyTupleTest()).await.unwrap();
}

#[tokio::test]
async fn write_struct() {
    let mut mock = Builder::new()
        .write_number(89)
        .write_slice(b"klomp")
        .build();
    mock.write_value(&StructTest {
        first: 89,
        second: String::from("klomp"),
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn write_tuple() {
    let mut mock = Builder::new()
        .write_number(89)
        .write_slice(b"klomp")
        .build();
    mock.write_value(&TupleTest(89, String::from("klomp")))
        .await
        .unwrap();
}

#[tokio::test]
async fn write_struct_version() {
    let mut mock = Builder::new()
        .version((1, 20))
        .write_number(89)
        .write_slice(b"klomp")
        .build();
    mock.write_value(&default_test()).await.unwrap();
}

#[tokio::test]
async fn write_struct_without_version() {
    let mut mock = Builder::new().version((1, 19)).write_number(89).build();
    mock.write_value(&StructVersionTest {
        test: 89,
        hello: String::new(),
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn write_tuple_version() {
    let mut mock = Builder::new()
        .version((1, 26))
        .write_number(89)
        .write_slice(b"klomp")
        .build();
    mock.write_value(&TupleVersionTest(89, "klomp".into()))
        .await
        .unwrap();
}

#[tokio::test]
async fn write_tuple_without_version() {
    let mut mock = Builder::new().version((1, 19)).write_number(89).build();
    mock.write_value(&TupleVersionTest(89, String::new()))
        .await
        .unwrap();
}

#[tokio::test]
async fn write_complex_1() {
    let mut mock = Builder::new()
        .version((1, 19))
        .write_number(999)
        .write_number(666)
        .build();
    mock.write_value(&TupleVersionDefaultTest(
        999,
        StructVersionTest {
            test: 666,
            hello: String::new(),
        },
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn write_complex_2() {
    let mut mock = Builder::new()
        .version((1, 20))
        .write_number(999)
        .write_number(666)
        .write_slice(b"The quick brown \xF0\x9F\xA6\x8A jumps over 13 lazy \xF0\x9F\x90\xB6.")
        .build();
    mock.write_value(&TupleVersionDefaultTest(
        999,
        StructVersionTest {
            test: 666,
            hello: String::from("The quick brown 🦊 jumps over 13 lazy 🐶."),
        },
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn write_complex_3() {
    let mut mock = Builder::new().version((1, 25)).write_number(999).build();
    mock.write_value(&TupleVersionDefaultTest(
        999,
        StructVersionTest {
            test: 89,
            hello: String::from("klomp"),
        },
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn write_complex_4() {
    let mut mock = Builder::new().version((1, 26)).write_number(999).build();
    mock.write_value(&TupleVersionDefaultTest(
        999,
        StructVersionTest {
            test: 89,
            hello: String::from("klomp"),
        },
    ))
    .await
    .unwrap();
}

#[derive(Debug, PartialEq, Eq, NixSerialize)]
#[nix(display)]
struct TestFromStr;

impl fmt::Display for TestFromStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "test")
    }
}

#[tokio::test]
async fn write_display() {
    let mut mock = Builder::new().write_display("test").build();
    mock.write_value(&TestFromStr).await.unwrap();
}

#[derive(Debug, PartialEq, Eq, NixSerialize)]
#[nix(display = "TestFromStr2::display")]
struct TestFromStr2;
struct TestFromStrDisplay;

impl fmt::Display for TestFromStrDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "test")
    }
}
impl TestFromStr2 {
    fn display(&self) -> TestFromStrDisplay {
        TestFromStrDisplay
    }
}

#[tokio::test]
async fn write_display_path() {
    let mut mock = Builder::new().write_display("test").build();
    mock.write_value(&TestFromStr2).await.unwrap();
}

#[derive(Debug, PartialEq, Eq, NixSerialize)]
#[nix(store_dir_display)]
struct TestFromStoreDirStr;

impl StoreDirDisplay for TestFromStoreDirStr {
    fn fmt(&self, _store_dir: &StoreDir, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "test")
    }
}

#[tokio::test]
async fn write_from_store_dir_str() {
    let mut mock = Builder::new().write_display("test").build();
    mock.write_value(&TestFromStoreDirStr).await.unwrap();
}

#[derive(Clone, Debug, PartialEq, Eq, NixSerialize)]
#[nix(try_into = "u64")]
struct TestTryFromU64(u64);

impl TryFrom<TestTryFromU64> for u64 {
    type Error = u64;

    fn try_from(value: TestTryFromU64) -> Result<Self, Self::Error> {
        if value.0 != 42 {
            Ok(value.0)
        } else {
            Err(value.0)
        }
    }
}

#[tokio::test]
async fn write_try_into_u64() {
    let mut mock = Builder::new().write_number(666).build();
    mock.write_value(&TestTryFromU64(666)).await.unwrap();
}

#[tokio::test]
async fn write_try_into_u64_invalid_data() {
    let mut mock = Builder::new().build();
    let err = mock.write_value(&TestTryFromU64(42)).await.unwrap_err();
    assert_eq!(Error::UnsupportedData("42".into()), err);
}

#[derive(Clone, Debug, PartialEq, Eq, NixSerialize)]
#[nix(into = "u64")]
struct TestFromU64;

impl From<TestFromU64> for u64 {
    fn from(_value: TestFromU64) -> u64 {
        42
    }
}

#[tokio::test]
async fn write_into_u64() {
    let mut mock = Builder::new().write_number(42).build();
    mock.write_value(&TestFromU64).await.unwrap();
}

#[derive(Debug, PartialEq, Eq, NixSerialize)]
enum TestEnum {
    #[nix(version = "..=19")]
    Pre20(TestFromU64, #[nix(version = "10..")] u64),
    #[nix(version = "20..=29")]
    Post20(StructVersionTest),
    #[nix(version = "30..=39")]
    Post30,
    #[nix(version = "40..")]
    Post40 {
        msg: String,
        #[nix(version = "45..")]
        level: u64,
    },
}

#[tokio::test]
async fn write_enum_9() {
    let mut mock = Builder::new().version((1, 9)).write_number(42).build();
    mock.write_value(&TestEnum::Pre20(TestFromU64, 666))
        .await
        .unwrap();
}

#[tokio::test]
async fn write_enum_19() {
    let mut mock = Builder::new()
        .version((1, 19))
        .write_number(42)
        .write_number(666)
        .build();
    mock.write_value(&TestEnum::Pre20(TestFromU64, 666))
        .await
        .unwrap();
}

#[tokio::test]
async fn write_enum_20() {
    let mut mock = Builder::new()
        .version((1, 20))
        .write_number(666)
        .write_slice(b"klomp")
        .build();
    mock.write_value(&TestEnum::Post20(StructVersionTest {
        test: 666,
        hello: "klomp".into(),
    }))
    .await
    .unwrap();
}

#[tokio::test]
async fn write_enum_30() {
    let mut mock = Builder::new().version((1, 30)).build();
    mock.write_value(&TestEnum::Post30).await.unwrap();
}

#[tokio::test]
async fn write_enum_40() {
    let mut mock = Builder::new()
        .version((1, 40))
        .write_slice(b"hello world")
        .build();
    mock.write_value(&TestEnum::Post40 {
        msg: "hello world".into(),
        level: 9001,
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn write_enum_45() {
    let mut mock = Builder::new()
        .version((1, 45))
        .write_slice(b"hello world")
        .write_number(9001)
        .build();
    mock.write_value(&TestEnum::Post40 {
        msg: "hello world".into(),
        level: 9001,
    })
    .await
    .unwrap();
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, IntoPrimitive, NixSerialize)]
#[nix(into = "u64")]
#[repr(u64)]
enum Tag {
    Pre20 = 1,
    Post20 = 2,
    Post30 = 3,
    Unknown = 4,
}

#[derive(Debug, PartialEq, Eq, NixSerialize)]
#[nix(tag = "Tag")]
enum TestTaggedEnum {
    Pre20(TestFromU64, #[nix(version = "10..")] u64),
    Post20(StructVersionTest),
    Post30,
    #[nix(tag = "Unknown")]
    Post40 {
        msg: String,
        #[nix(version = "45..")]
        level: u64,
    },
}

#[tokio::test]
async fn write_tagged_enum_tuple_fields_1() {
    let mut mock = Builder::new()
        .version((1, 9))
        .write_number(1)
        .write_number(42)
        .build();
    mock.write_value(&TestTaggedEnum::Pre20(TestFromU64, 666))
        .await
        .unwrap();
}

#[tokio::test]
async fn write_tagged_enum_tuple_fields_2() {
    let mut mock = Builder::new()
        .version((1, 19))
        .write_number(1)
        .write_number(42)
        .write_number(666)
        .build();
    mock.write_value(&TestTaggedEnum::Pre20(TestFromU64, 666))
        .await
        .unwrap();
}

#[tokio::test]
async fn write_tagged_enum_struct() {
    let mut mock = Builder::new()
        .version((1, 20))
        .write_number(2)
        .write_number(666)
        .write_slice(b"klomp")
        .build();
    mock.write_value(&TestTaggedEnum::Post20(StructVersionTest {
        test: 666,
        hello: "klomp".into(),
    }))
    .await
    .unwrap();
}

#[tokio::test]
async fn write_tagged_enum_unit() {
    let mut mock = Builder::new().version((1, 30)).write_number(3).build();
    mock.write_value(&TestTaggedEnum::Post30).await.unwrap();
}

#[tokio::test]
async fn write_tagged_enum_struct_fields_1() {
    let mut mock = Builder::new()
        .version((1, 40))
        .write_number(4)
        .write_slice(b"hello world")
        .build();
    mock.write_value(&TestTaggedEnum::Post40 {
        msg: "hello world".into(),
        level: 9001,
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn write_tagged_enum_struct_fields_2() {
    let mut mock = Builder::new()
        .version((1, 45))
        .write_number(4)
        .write_slice(b"hello world")
        .write_number(9001)
        .build();
    mock.write_value(&TestTaggedEnum::Post40 {
        msg: "hello world".into(),
        level: 9001,
    })
    .await
    .unwrap();
}
