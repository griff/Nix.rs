pub trait NumEnum: Sized {
    type Rep: Sized;
    const REP_SIZE: usize = std::mem::size_of::<Self::Rep>();
    const REP_BITS: usize = Self::REP_SIZE * 8;

    fn members() -> Vec<(Self, Self::Rep)>;
}

macro_rules! num_enum {
    (
        $( #[$meta:meta] )*
        $vis:vis enum $name:ident {
            $u:ident($t:ty)
            $(,$i:ident = $v:literal)+$(,)?
        }
    ) => {
        $( #[$meta] )*
        $vis enum $name {
            $($i),+,
            $u($t),
        }
        impl $name {
            pub fn value(&self) -> $t {
                self.into()
            }
        }
        impl From<$t> for $name {
            fn from(value: $t) -> $name {
                match value {
                    $($v => $name::$i,)+
                    x => $name::$u(x),
                }
            }
        }
        impl From<$name> for $t {
            fn from(value: $name) -> $t {
                match value {
                    $($name::$i => $v,)+
                    $name::$u(x) => x
                }
            }
        }
        impl<'a> From<&'a $name> for $t {
            fn from(value: &'a $name) -> $t {
                match value {
                    $($name::$i => $v,)+
                    $name::$u(x) => *x
                }
            }
        }
        impl $crate::num_enum::NumEnum for $name {
            type Rep = $t;

            fn members() -> Vec<($name, $t)> {
                vec![$(($name::$i, $v)),+]
            }
        }
    }
}
pub(crate) use num_enum;
