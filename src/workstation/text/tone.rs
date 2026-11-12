//! The colour roles the painters agree on, as names. Each painter decides what a role looks like, so
//! the words and the roles are stated once and the hues are not.

/// A role a piece of text or a shape plays, not a hue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Tone {
    Text,
    Text2,
    Bid,
    Ask,
    SellFill,
    Positive,
    Negative,
    Warning,
    Invalid,
    Stale,
    Selected,
}
