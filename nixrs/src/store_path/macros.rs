macro_rules! partial_eq_self {
    ($own:ty) => {
        impl PartialEq<&$own> for $own {
            fn eq(&self, other: &&$own) -> bool {
                self == **other
            }
        }
        impl PartialEq<$own> for &$own {
            fn eq(&self, other: &$own) -> bool {
                *self == other
            }
        }
    };
}
pub(crate) use partial_eq_self;

macro_rules! partial_eq {
    ($own:ty, $ty:ty) => {
        impl PartialEq<$ty> for $own {
            fn eq(&self, other: &$ty) -> bool {
                self.0 == *other
            }
        }
        impl PartialEq<$own> for $ty {
            fn eq(&self, other: &$own) -> bool {
                *self == other.0
            }
        }
    };
}
pub(crate) use partial_eq;

macro_rules! partial_eq_ref {
    ($own:ty, $ty:ty) => {
        impl PartialEq<$ty> for $own {
            fn eq(&self, other: &$ty) -> bool {
                **self == **other
            }
        }
        impl PartialEq<$own> for $ty {
            fn eq(&self, other: &$own) -> bool {
                **self == **other
            }
        }
    };
}
pub(crate) use partial_eq_ref;
