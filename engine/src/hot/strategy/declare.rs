//! One declaration per ordered set of named f64 slots: the names, the kinds, the resolved struct
//! and the flattening are the same list, so a slot cannot drift from the name that labels it on disk
//! or on the wire. Two macros rather than one — a feature table resolves ids the engine assigned,
//! a link schema builds a payload; same shape, different contracts.
//! Macro bodies sit outside the fmt gate (rustfmt skips brace-delimited item macros) — hand-formatted.

/// Column names, their kinds, and the ids registration resolved them to, from one list of
/// `name: kind` lines, where a kind is `market_only` or `execution_aware`. `from_ids` consumes the
/// registration slice in declaration order, which is the order the engine assigned ids in.
#[macro_export]
macro_rules! features {
    (
        $(#[$struct_meta:meta])*
        $vis:vis struct $ty:ident {
            $($name:ident : $kind:ident),+ $(,)?
        }
        $(#[$names_meta:meta])*
        $names_vis:vis const $names:ident;
    ) => {
        $(#[$names_meta])*
        $names_vis const $names: &[&str] = &[$(stringify!($name)),+];

        $(#[$struct_meta])*
        #[derive(Debug, Clone, Copy)]
        $vis struct $ty {
            $(
                // The on-disk contract is append-only, so a column outlives the code that emitted
                // it: an id nothing reads is a designed state, not a leftover.
                #[allow(dead_code)]
                $vis $name: $crate::hot::strategy::FeatureId,
            )+
        }

        impl $ty {
            /// The kind of each column, in the order of the names.
            #[allow(dead_code)]
            $vis const KINDS: &[$crate::hot::strategy::FeatureKind] =
                &[$($crate::feature_kind!($kind)),+];

            $vis fn from_ids(ids: &[$crate::hot::strategy::FeatureId]) -> Self {
                assert_eq!(
                    ids.len(),
                    $names.len(),
                    "engine assigned {} feature ids for the {} names {} declares",
                    ids.len(),
                    $names.len(),
                    stringify!($ty)
                );
                let mut next = ids.iter().copied();
                Self {
                    $($name: next.next().expect("length checked above"),)+
                }
            }
        }
    };
}

/// One kind word of a `features!` line. Any other word fails to compile, so no column is
/// declared without a kind.
#[doc(hidden)]
#[macro_export]
macro_rules! feature_kind {
    (market_only) => {
        $crate::hot::strategy::FeatureKind::MarketOnly
    };
    (execution_aware) => {
        $crate::hot::strategy::FeatureKind::ExecutionAware
    };
}

/// Link payload from one `$wire => $slot` list per group: the name that rides the wire and the
/// field holding its value are the same line, and groups flatten in authored order — that order IS
/// the wire layout the peer's digest agrees on.
#[macro_export]
macro_rules! link_schema {
    (
        $(#[$role_meta:meta])*
        $role_vis:vis struct $role:ident {
            $($slot:ident),+ $(,)?
        }
        $(#[$frame_meta:meta])*
        $frame_vis:vis struct $frame:ident {
            $($group:ident: { $($wire:ident => $line:ident),+ $(,)? }),+ $(,)?
        }
        $(#[$names_meta:meta])*
        $names_vis:vis const $names:ident;
    ) => {
        // One schema file compiles into both roots, hence `dead_code` allowed throughout: the
        // sender reads only the frame, the receiver only the name list.
        $(#[$names_meta])*
        #[allow(dead_code)]
        $names_vis const $names: [&str; { [$($(stringify!($wire),)+)+].len() }] =
            [$($(stringify!($wire),)+)+];

        const _: () = assert!(
            $names.len() <= $crate::link::LINK_MAX_FIELDS,
            "link schema declares more slots than a link frame carries"
        );

        const _: () = assert!(
            $names.len()
                == [$(stringify!($slot),)+].len() * [$(stringify!($group),)+].len(),
            "every group must map every role slot exactly once — an unmapped slot would hold a \
             value the wire silently never carries"
        );

        const _: () = {
            let mut index = 0;
            while index < $names.len() {
                assert!(
                    $names[index].len() <= $crate::link::LINK_NAME_LEN,
                    "link field name exceeds LINK_NAME_LEN — this moves the boot-time name panic to \
                     compile time"
                );
                index += 1;
            }
        };

        $(#[$role_meta])*
        #[derive(Debug, Clone, Copy)]
        #[allow(dead_code)]
        $role_vis struct $role {
            $($role_vis $slot: f64,)+
        }

        impl $role {
            /// Absent, never zero — a derived `Default` would seed 0.0, which the receiver reads as
            /// a live price rather than a missing one.
            #[allow(dead_code)]
            $role_vis const ABSENT: Self = Self { $($slot: f64::NAN,)+ };
        }

        $(#[$frame_meta])*
        #[derive(Debug, Clone, Copy)]
        #[allow(dead_code)]
        $frame_vis struct $frame {
            $($frame_vis $group: $role,)+
        }

        impl $frame {
            #[allow(dead_code)]
            $frame_vis const ABSENT: Self = Self { $($group: $role::ABSENT,)+ };

            #[allow(dead_code)]
            $frame_vis fn to_array(&self) -> [f64; $names.len()] {
                [$($(self.$group.$line,)+)+]
            }
        }
    };
}

/// The params of a strategy from one list: each knob with its unit, its default, and, after
/// `within`, the bounds that it must keep. The config parses each value from its text, so an exact
/// amount never passes through a float, and a key that no line names or a value outside its bounds
/// refuses the start and names the knob.
///
/// ```ignore
/// params! {
///     pub struct MakerParams {
///         order_size: Qty = 5 within 5 ..= 1000,
///         edge: Ticks = 2 within 0 ..= 50,
///         enabled: bool = false,
///     }
/// }
/// ```
#[macro_export]
macro_rules! params {
    (
        $(#[$struct_meta:meta])*
        $vis:vis struct $ty:ident {
            $(
                $(#[$field_meta:meta])*
                $field:ident : $unit:ty = $default:literal $(within $low:literal ..= $high:literal)?
            ),+ $(,)?
        }
    ) => {
        $(#[$struct_meta])*
        #[derive(Debug, Clone, Copy, PartialEq, ::serde::Deserialize, ::serde::Serialize)]
        #[serde(default)]
        $vis struct $ty {
            $(
                $(#[$field_meta])*
                pub $field: $unit,
            )+
        }

        impl ::core::default::Default for $ty {
            fn default() -> Self {
                Self {
                    $(
                        $field: $crate::hot::strategy::param_literal::<$unit>(
                            stringify!($field),
                            stringify!($default),
                        ),
                    )+
                }
            }
        }

        impl $crate::hot::strategy::StrategyParams for $ty {
            fn check(&self) -> ::core::result::Result<(), $crate::hot::strategy::ParamError> {
                $($(
                    $crate::hot::strategy::check_bounds(
                        stringify!($field),
                        self.$field,
                        stringify!($low),
                        stringify!($high),
                    )?;
                )?)+
                Ok(())
            }
        }
    };
}

/// The signals of a strategy from one list: one struct for each kind of signal, each field a value
/// of its unit that is not ready until the strategy assigns it, and one list of the names and the
/// kinds of all of them, in order, which the strategy hands the engine as its features.
/// [`StrategyCtx::record`](crate::hot::strategy::StrategyCtx::record) records each assigned field.
///
/// ```ignore
/// signals! {
///     const SIGNALS;
///     market_only struct Micro { mid: Fair, spread: Bps }
///     execution_aware struct Model { bid_price: Price }
/// }
/// ```
#[macro_export]
macro_rules! signals {
    (
        $(#[$list_meta:meta])*
        $list_vis:vis const $list:ident;
        $(
            $(#[$struct_meta:meta])*
            $kind:ident struct $ty:ident {
                $($field:ident : $unit:ty),+ $(,)?
            }
        )+
    ) => {
        $(#[$list_meta])*
        $list_vis const $list: $crate::hot::strategy::SignalList = $crate::hot::strategy::SignalList {
            names: &[$($(stringify!($field)),+),+],
            kinds: &[$($($crate::signal_kind!($kind, $field)),+),+],
        };

        const _: () = assert!(
            $list.has_unique_names(),
            "signals! declares one name twice, and a name is the column of one signal"
        );

        $(
            $(#[$struct_meta])*
            #[derive(Debug, Clone, Copy, PartialEq)]
            pub struct $ty {
                $(pub $field: $crate::hot::strategy::Ready<$unit>,)+
            }

            impl ::core::default::Default for $ty {
                fn default() -> Self {
                    Self {
                        $(
                            $field: Err($crate::hot::strategy::NotReady::Signal(
                                stringify!($field),
                            )),
                        )+
                    }
                }
            }

            impl $crate::hot::strategy::SignalSet for $ty {
                fn record_into(
                    &self,
                    instrument: $crate::msg::ids::InstrumentId,
                    ctx: &mut $crate::hot::strategy::StrategyCtx<'_>,
                ) {
                    $(
                        if let Ok(value) = self.$field {
                            let feature = const { $list.id_of(stringify!($field)) };
                            ctx.emit(
                                feature,
                                instrument,
                                $crate::hot::strategy::SignalValue::recorded(value),
                            );
                        }
                    )+
                }
            }
        )+
    };
}

/// One kind word of a `signals!` struct. Any other word fails to compile.
#[doc(hidden)]
#[macro_export]
macro_rules! signal_kind {
    (market_only, $field:ident) => {
        $crate::hot::strategy::FeatureKind::MarketOnly
    };
    (execution_aware, $field:ident) => {
        $crate::hot::strategy::FeatureKind::ExecutionAware
    };
}
