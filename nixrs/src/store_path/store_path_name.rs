use std::borrow::{Borrow, Cow};
use std::ops::Deref;
use std::str::FromStr;

use crate::store_path::macros::{partial_eq, partial_eq_ref};

use super::macros::partial_eq_self;

const NAME_LOOKUP: [bool; 256] = {
    let mut ret = [false; 256];
    let mut idx = 0usize;
    while idx < u8::MAX as usize {
        let ch = idx as u8;
        ret[idx] = matches!(ch, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'+' | b'-' | b'_' | b'?' | b'=' | b'.');
        idx += 1;
    }
    ret
};
pub const MAX_NAME_LEN: usize = 211;

fn into_name(s: &[u8]) -> Result<&StorePathNameRef, StorePathNameError> {
    if s.is_empty() || s.len() > MAX_NAME_LEN {
        return Err(StorePathNameError::NameLength);
    }

    for (position, ch) in s.iter().enumerate() {
        if !NAME_LOOKUP[*ch as usize] {
            return Err(StorePathNameError::Symbol {
                position,
                symbol: *ch,
            });
        }
    }

    // SAFETY: We checked above that it is a subset of ASCII, which guarantees valid UTF-8.
    let name = unsafe { std::str::from_utf8_unchecked(s) };
    // SAFETY: We have justed checked that it is a name
    let ret = unsafe { StorePathNameRef::from_str_unchecked(name) };
    Ok(ret)
}

pub const DRV_EXT: &str = ".drv";

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::Display)]
#[repr(transparent)]
pub struct StorePathNameRef(str);

impl StorePathNameRef {
    pub const fn max_len() -> usize {
        MAX_NAME_LEN
    }

    /// Converts a str to a StorePathNameRef without checking that it is a valid name.
    ///
    /// # Safety
    /// This function is unsafe because it does not check that the str passed to it is a
    /// valid store path name. If this constraint is violated it may cause panics and memory safety
    /// issues , as the rest of nixrs assumes that `StorePathNameRef` contains a valid store path name.
    pub const unsafe fn from_str_unchecked(name: &str) -> &StorePathNameRef {
        // SAFETY: `str` and `StorePathNameRef` have the same ABI because of repr(transparent)
        unsafe { &*(name as *const str as *const StorePathNameRef) }
    }

    #[expect(clippy::should_implement_trait)]
    pub fn from_str(name: &str) -> Result<&StorePathNameRef, StorePathNameError> {
        into_name(name.as_bytes())
    }

    pub fn from_slice(name: &[u8]) -> Result<&StorePathNameRef, StorePathNameError> {
        into_name(name)
    }

    pub fn is_drv(&self) -> bool {
        self.0.ends_with(DRV_EXT)
    }

    pub fn strip_drv_ext(&self) -> Option<&StorePathNameRef> {
        self.strip_suffix(DRV_EXT)
    }

    pub fn strip_suffix(&self, suffix: &str) -> Option<&StorePathNameRef> {
        if let Some(new_name) = self.0.strip_suffix(suffix)
            && !new_name.is_empty()
        {
            // SAFETY: When remove from a name it is always shorter and still contains valid characters
            // The only exception is when all characters were stripped and that is checked above
            unsafe { Some(StorePathNameRef::from_str_unchecked(new_name)) }
        } else {
            None
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl AsRef<str> for StorePathNameRef {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for StorePathNameRef {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'a> TryFrom<&'a [u8]> for &'a StorePathNameRef {
    type Error = StorePathNameError;

    fn try_from(name: &'a [u8]) -> Result<Self, Self::Error> {
        StorePathNameRef::from_slice(name)
    }
}

impl<'a> TryFrom<&'a str> for &'a StorePathNameRef {
    type Error = StorePathNameError;

    fn try_from(name: &'a str) -> Result<Self, Self::Error> {
        StorePathNameRef::from_str(name)
    }
}

impl ToOwned for StorePathNameRef {
    type Owned = StorePathName;

    fn to_owned(&self) -> Self::Owned {
        StorePathName(self.0.to_owned())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::Display)]
pub struct StorePathName(String);

impl StorePathName {
    pub const fn max_len() -> usize {
        MAX_NAME_LEN
    }

    pub fn from_string(name: String) -> Result<Self, StorePathNameError> {
        into_name(name.as_bytes())?;
        Ok(Self(name))
    }

    pub fn from_slice(name: &[u8]) -> Result<Self, StorePathNameError> {
        let name = into_name(name)?;
        Ok(name.into())
    }

    pub fn as_name_ref(&self) -> &StorePathNameRef {
        // SAFETY: A StorePathName always contains a valid store path name
        unsafe { StorePathNameRef::from_str_unchecked(&self.0) }
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl TryFrom<String> for StorePathName {
    type Error = StorePathNameError;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        StorePathName::from_string(name)
    }
}

impl TryFrom<&[u8]> for StorePathName {
    type Error = StorePathNameError;

    fn try_from(name: &[u8]) -> Result<Self, Self::Error> {
        StorePathName::from_slice(name)
    }
}

impl FromStr for StorePathName {
    type Err = StorePathNameError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        StorePathName::from_slice(name.as_bytes())
    }
}

impl AsRef<str> for StorePathName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<StorePathNameRef> for StorePathName {
    fn as_ref(&self) -> &StorePathNameRef {
        self.as_name_ref()
    }
}

impl Deref for StorePathName {
    type Target = StorePathNameRef;

    fn deref(&self) -> &Self::Target {
        self.as_name_ref()
    }
}

impl Borrow<StorePathNameRef> for StorePathName {
    fn borrow(&self) -> &StorePathNameRef {
        self.as_name_ref()
    }
}

impl<'a> From<&'a StorePathNameRef> for StorePathName {
    fn from(value: &'a StorePathNameRef) -> Self {
        value.to_owned()
    }
}

partial_eq_self!(StorePathName);
partial_eq!(StorePathName, &'_ str);
partial_eq!(StorePathName, String);
partial_eq!(StorePathName, Cow<'_, str>);
partial_eq_ref!(StorePathName, &'_ StorePathNameRef);
partial_eq_ref!(StorePathName, Cow<'_, StorePathNameRef>);

#[derive(Debug, PartialEq, Eq, Clone, thiserror::Error)]
pub enum StorePathNameError {
    #[error("invalid store path name length")]
    NameLength,
    #[error("invalid store path name symbol {ch} at position {position}", ch = super::display_symbol(*symbol))]
    Symbol { position: usize, symbol: u8 },
}

impl StorePathNameError {
    pub fn adjust_index(self, prefix: usize) -> StorePathNameError {
        match self {
            StorePathNameError::Symbol { position, symbol } => StorePathNameError::Symbol {
                position: prefix + position,
                symbol,
            },
            StorePathNameError::NameLength => StorePathNameError::NameLength,
        }
    }
}

#[cfg(test)]
mod unittests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("perl5.38.0-libnet-3.12")]
    #[case::all(".-_?+=abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSSTUVWXYZ")]
    #[case::dot(".")]
    #[case::dotdot("..")]
    #[case::dotdash(".-")]
    #[case::dotdotdash("..-")]
    #[case::longest(
        "test-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    )]
    fn name_ok(#[case] case: &str) {
        let name = case.parse::<StorePathName>().expect("parses");
        let name_s: &str = name.as_ref();
        assert_eq!(case, name.to_string());
        assert_eq!(case, name);
        assert_eq!(case.to_string(), name);
        assert_eq!(Cow::Borrowed(case), name);
        assert_eq!(case, name_s);
        assert_eq!(case.as_bytes(), name.as_bytes());
        let name2: StorePathName = case.as_bytes().try_into().expect("parses bytes");
        assert_eq!(name, name2);
    }

    #[rstest]
    #[should_panic(expected = "invalid store path name length")]
    #[case::empty("")]
    #[should_panic(expected = "invalid store path name length")]
    #[case::too_long(
        "test-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    )]
    #[should_panic(expected = "invalid store path name symbol '|' at position 4")]
    #[case::invalid_char("test|more")]
    fn name_errors(#[case] name: &str) {
        let err = name.parse::<StorePathName>().expect_err("parse succeeded");
        panic!("{err}");
    }
}

#[cfg(test)]
mod proptests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn proptest_store_name_parse_display(path in any::<StorePathName>()) {
            let s = path.to_string();
            let parsed = s.parse::<StorePathName>().expect("Parsing display");
            prop_assert_eq!(path, parsed);
        }
    }
}
