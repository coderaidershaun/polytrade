//! One declaration per ordered set of named f64 slots: the names, the resolved struct and the
//! flattening are the same ident list, so a slot cannot drift from the name that labels it on disk
//! or on the wire. Two macros rather than one — a feature table resolves ids the engine assigned,
//! a link schema builds a payload; same shape, different contracts.
//! Macro bodies sit outside the fmt gate (rustfmt skips brace-delimited item macros) — hand-formatted.

/// Column names and the ids registration resolved them to, from one ident list. `from_ids` consumes
/// the registration slice in declaration order, which is the order the engine assigned ids in.
#[macro_export]
macro_rules! features {
    (
        $(#[$struct_meta:meta])*
        $vis:vis struct $ty:ident {
            $($name:ident),+ $(,)?
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
