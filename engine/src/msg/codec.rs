//! The digest of a fixed binary layout, folded at compile time from the names, widths and tags of
//! its parts. The link and the tape each digest their layouts by these rules, so one rule decides
//! when two builds can read each other's bytes.

use crate::msg::link::{FNV_OFFSET_BASIS, fnv_fold};

/// Folds the parts of a layout into one digest at compile time.
#[derive(Clone, Copy)]
pub(crate) struct LayoutDigest(u64);

impl LayoutDigest {
    pub(crate) const START: Self = Self(FNV_OFFSET_BASIS);

    /// ASCII whitespace is skipped, so the spacing that `stringify!` gives cannot move the digest. A
    /// zero byte ends each text, so two names cannot run into one.
    pub(crate) const fn text(self, text: &str) -> Self {
        let bytes = text.as_bytes();
        let mut hash = self.0;
        let mut index = 0;
        while index < bytes.len() {
            if !bytes[index].is_ascii_whitespace() {
                hash = fnv_fold(hash, &[bytes[index]]);
            }
            index += 1;
        }
        Self(fnv_fold(hash, &[0]))
    }

    pub(crate) const fn number(self, value: u64) -> Self {
        Self(fnv_fold(self.0, &value.to_le_bytes()))
    }

    /// One part of the layout: its width and its own digest.
    pub(crate) const fn part(self, width: usize, digest: u64) -> Self {
        self.number(width as u64).number(digest)
    }

    pub(crate) const fn finish(self) -> u64 {
        self.0
    }
}
