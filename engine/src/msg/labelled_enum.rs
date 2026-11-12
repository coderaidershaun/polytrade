//! One declaration per string-tagged enum: the variant list, its label table and its exhaustive
//! `ALL` array are the same text, so a variant cannot be added without a label, and `ALL` cannot
//! fall out of the declaration order that `variant as usize` indexes by.
//! Macro bodies sit outside the fmt gate (rustfmt skips brace-delimited item macros) — hand-formatted.

/// Trailing `const ALL` is opt-in: an unread `ALL` would be a public promise nobody asked for.
macro_rules! labelled_enum {
    (
        $(#[$enum_meta:meta])*
        $vis:vis enum $ty:ident {
            $( $(#[$variant_meta:meta])* $variant:ident = $label:literal ),+ $(,)?
        }
        $(#[$fn_meta:meta])*
        $fn_vis:vis fn $accessor:ident;
        $(#[$all_meta:meta])*
        $all_vis:vis const ALL;
    ) => {
        labelled_enum! {
            $(#[$enum_meta])*
            $vis enum $ty { $( $(#[$variant_meta])* $variant = $label ),+ }
            $(#[$fn_meta])*
            $fn_vis fn $accessor;
        }

        impl $ty {
            $(#[$all_meta])*
            $all_vis const ALL: [$ty; { [$( $ty::$variant ),+].len() }] = [$( $ty::$variant ),+];
        }
    };
    (
        $(#[$enum_meta:meta])*
        $vis:vis enum $ty:ident {
            $( $(#[$variant_meta:meta])* $variant:ident = $label:literal ),+ $(,)?
        }
        $(#[$fn_meta:meta])*
        $fn_vis:vis fn $accessor:ident;
    ) => {
        $(#[$enum_meta])*
        $vis enum $ty {
            $( $(#[$variant_meta])* $variant ),+
        }

        impl $ty {
            $(#[$fn_meta])*
            $fn_vis fn $accessor(self) -> &'static str {
                match self {
                    $( $ty::$variant => $label ),+
                }
            }
        }
    };
}

pub(crate) use labelled_enum;
