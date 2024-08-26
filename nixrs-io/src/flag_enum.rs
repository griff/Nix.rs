#[macro_export]
macro_rules! flag_enum {
    (
        $( #[$meta:meta] )*
        $vis:vis enum $name:ident {
            $if:ident = false,
            $it:ident = true$(,)?
        }
    ) => {
        flag_enum! {
            $( #[$meta] )*
            $vis enum $name {
                $it = true,
                $if = false
            }
        }
    };
    (
        $( #[$meta:meta] )*
        $vis:vis enum $name:ident {
            $it:ident = true,
            $if:ident = false$(,)?
        }
    ) => {
        $( #[$meta] )*
        $vis enum $name {
            $it,
            $if
        }
        impl std::ops::Not for $name {
            type Output = $name;
            fn not(self) -> Self::Output {
                match self {
                    $name::$it => $name::$if,
                    $name::$if => $name::$it,
                }
            }
        }
        impl From<bool> for $name {
            fn from(v: bool) -> $name {
                if v {
                    $name::$it
                } else {
                    $name::$if
                }
            }
        }
        impl From<$name> for bool {
            fn from(v: $name) -> bool {
                match v {
                    $name::$it => true,
                    $name::$if => false,
                }
            }
        }
    }
}