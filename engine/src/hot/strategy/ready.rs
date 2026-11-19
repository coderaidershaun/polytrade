//! A value that a strategy reads, or the reason it cannot have it yet. One `?` passes the reason up,
//! so the side that needed the value does not quote this spin, and the strategy writes no branch.

/// The value, or why it is not ready this spin.
pub type Ready<T> = Result<T, NotReady>;

/// Why a value is not ready. Each reason names the one input that is missing.
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NotReady {
    #[error("the market is warming up")]
    Warming,
    #[error("the run is parked")]
    Parked,
    #[error("the book is not valid")]
    BookInvalid,
    #[error("the book has no bid")]
    NoBid,
    #[error("the book has no ask")]
    NoAsk,
    #[error("the position has no mark yet")]
    NoMark,
    #[error("the market has no window yet")]
    NoWindow,
    #[error("the position is below the venue's minimum order size")]
    BelowMinimum,
    #[error("the price is off the grid of the market")]
    OffGrid,
    #[error("signal {0} has no value yet")]
    Signal(&'static str),
}
