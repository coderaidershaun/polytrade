//! The slots in use of a slot table, one bit for each slot, so a pass visits the orders that exist
//! rather than each slot of the table.

use std::ops::Range;

const WORD_BITS: usize = 64;

/// A bit is set while its slot is not free. Each walk goes in ascending slot order, the order of a
/// scan of the whole table, so a pass that walks it banks its commands in the same order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SlotsInUse {
    words: Box<[u64]>,
}

impl SlotsInUse {
    pub(super) fn with_slots(slots: usize) -> Self {
        Self {
            words: vec![0; slots.div_ceil(WORD_BITS)].into_boxed_slice(),
        }
    }

    #[inline]
    pub(super) fn insert(&mut self, slot: usize) {
        self.words[slot / WORD_BITS] |= 1 << (slot % WORD_BITS);
    }

    #[inline]
    pub(super) fn remove(&mut self, slot: usize) {
        self.words[slot / WORD_BITS] &= !(1 << (slot % WORD_BITS));
    }

    /// The first slot in use at or after `from`. A pass that changes slots as it goes walks with
    /// this, one call for each step, since it cannot hold the set borrowed.
    #[inline]
    pub(super) fn next_from(&self, from: usize) -> Option<usize> {
        let mut word = from / WORD_BITS;
        let mut bits = *self.words.get(word)? & (u64::MAX << (from % WORD_BITS));
        while bits == 0 {
            word += 1;
            bits = *self.words.get(word)?;
        }
        Some(word * WORD_BITS + bits.trailing_zeros() as usize)
    }

    #[inline]
    pub(super) fn within(&self, range: Range<usize>) -> impl Iterator<Item = usize> + '_ {
        let mut next = self.next_from(range.start);
        std::iter::from_fn(move || {
            let slot = next.filter(|slot| *slot < range.end)?;
            next = self.next_from(slot + 1);
            Some(slot)
        })
    }

    #[inline]
    pub(super) fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.within(0..self.words.len() * WORD_BITS)
    }
}
