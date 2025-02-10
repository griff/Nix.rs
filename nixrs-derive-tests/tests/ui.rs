use std::path::PathBuf;

use rstest::rstest;

#[rstest]
#[test]
fn ui(#[files("tests/ui/*.rs")] path: PathBuf) {
    let t = trybuild::TestCases::new();
    t.compile_fail(path);
}
