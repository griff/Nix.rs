macro_rules! num_enum {
    (
        $( #[$meta:meta] )*
        $vis:vis enum $name:ident {
            $u:ident($t:ty)
            $(,$(#[$metai:meta])*
            $i:ident = $v:literal)+$(,)?
        }
    ) => {
        $( #[$meta] )*
        $vis enum $name {
            $( $(#[$metai])* $i ),+,
            $u($t),
        }
        impl $name {
            #[allow(unused)]
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
    }
}
pub(crate) use num_enum;
