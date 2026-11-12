//! Queue-position tracking for public and simulated liquidity.

use crate::ids::{Price, Qty, Side};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SimOrderIndex(pub u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueueAhead {
    Known(Qty),
    Unobservable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueueNode {
    Public(QueueAhead),
    Own(SimOrderIndex),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PublicPolicy {
    Consume,
    Skip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OwnFill {
    pub taken: Qty,
    pub is_complete: bool,
}

#[derive(Debug, Clone, Default)]
pub struct PriceQueue {
    nodes: Vec<QueueNode>,
}

impl PriceQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn holds_own_liquidity(&self) -> bool {
        self.nodes
            .iter()
            .any(|node| matches!(node, QueueNode::Own(_)))
    }

    pub fn has_unobservable_public(&self) -> bool {
        self.nodes
            .iter()
            .any(|node| matches!(node, QueueNode::Public(QueueAhead::Unobservable)))
    }

    pub fn known_public_qty(&self) -> Qty {
        Qty(self
            .nodes
            .iter()
            .map(|node| match node {
                QueueNode::Public(QueueAhead::Known(qty)) => qty.0,
                _ => 0,
            })
            .sum())
    }

    pub fn public_ahead_of(&self, index: SimOrderIndex) -> Option<QueueAhead> {
        let mut ahead = Qty(0);
        for node in &self.nodes {
            match node {
                QueueNode::Own(owner) if *owner == index => return Some(QueueAhead::Known(ahead)),
                QueueNode::Public(QueueAhead::Unobservable) => {
                    return Some(QueueAhead::Unobservable);
                }
                QueueNode::Public(QueueAhead::Known(qty)) => ahead = Qty(ahead.0 + qty.0),
                _ => {}
            }
        }
        None
    }

    pub fn push_public(&mut self, ahead: QueueAhead) {
        self.nodes.push(QueueNode::Public(ahead));
    }

    pub fn push_own(&mut self, index: SimOrderIndex) {
        self.nodes.push(QueueNode::Own(index));
    }

    pub fn reconcile_known_public_to(&mut self, visible: Qty) {
        if self.has_unobservable_public() {
            return;
        }
        let known = self.known_public_qty();
        if known.0 < visible.0 {
            self.push_public(QueueAhead::Known(Qty(visible.0 - known.0)));
            return;
        }
        self.shrink_known_public_from_tail(Qty(known.0 - visible.0));
    }

    fn shrink_known_public_from_tail(&mut self, mut excess: Qty) {
        let mut at = self.nodes.len();
        while at > 0 && excess.0 > 0 {
            at -= 1;
            let QueueNode::Public(QueueAhead::Known(qty)) = self.nodes[at] else {
                continue;
            };
            let remaining = remaining_after(&mut excess, qty);
            match remaining.0 {
                0 => {
                    self.nodes.remove(at);
                }
                _ => self.nodes[at] = QueueNode::Public(QueueAhead::Known(remaining)),
            }
        }
        debug_assert_eq!(
            excess.0, 0,
            "tail-first shrink left {} unexplained",
            excess.0
        );
    }

    pub fn mark_public_unobservable(&mut self) {
        self.nodes
            .retain(|node| !matches!(node, QueueNode::Public(_)));
        if self.holds_own_liquidity() {
            self.nodes
                .insert(0, QueueNode::Public(QueueAhead::Unobservable));
        }
    }

    pub fn remove_own(&mut self, index: SimOrderIndex) {
        let own = self
            .nodes
            .iter()
            .position(|node| matches!(node, QueueNode::Own(owner) if *owner == index));
        if let Some(at) = own {
            self.nodes.remove(at);
        }
    }

    pub fn walk(
        &mut self,
        public: PublicPolicy,
        budget: &mut Qty,
        take: &mut dyn FnMut(SimOrderIndex, Qty) -> OwnFill,
    ) {
        let mut at = 0;
        while at < self.nodes.len() && budget.0 > 0 {
            let is_consumed = match self.nodes[at] {
                QueueNode::Public(QueueAhead::Unobservable) => {
                    if public == PublicPolicy::Consume {
                        return;
                    }
                    false
                }
                QueueNode::Public(QueueAhead::Known(qty)) if public == PublicPolicy::Consume => {
                    let remaining = remaining_after(budget, qty);
                    self.nodes[at] = QueueNode::Public(QueueAhead::Known(remaining));
                    remaining.0 == 0
                }
                QueueNode::Public(QueueAhead::Known(_)) => false,
                QueueNode::Own(index) => {
                    let fill = take(index, *budget);
                    assert!(
                        (0..=budget.0).contains(&fill.taken.0),
                        "own fill {} is outside the aggressor budget 0..={}",
                        fill.taken.0,
                        budget.0
                    );
                    *budget = Qty(budget.0 - fill.taken.0);
                    fill.is_complete
                }
            };
            match is_consumed {
                true => {
                    self.nodes.remove(at);
                }
                false => at += 1,
            }
        }
    }
}

/// Spends what it can of `budget` against `available`, answering with the part of `available` the
/// budget could not reach — the two are not interchangeable, and the name says which comes back.
fn remaining_after(budget: &mut Qty, available: Qty) -> Qty {
    let taken = Qty(available.0.min(budget.0));
    *budget = Qty(budget.0 - taken.0);
    Qty(available.0 - taken.0)
}

#[derive(Debug, Clone)]
pub struct PriceLadder {
    side: Side,
    levels: Vec<(Price, PriceQueue)>,
}

impl PriceLadder {
    pub fn new(side: Side) -> Self {
        Self {
            side,
            levels: Vec::new(),
        }
    }

    pub fn side(&self) -> Side {
        self.side
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (Price, &mut PriceQueue)> {
        self.levels.iter_mut().map(|(price, queue)| (*price, queue))
    }

    pub fn queue(&self, price: Price) -> Option<&PriceQueue> {
        self.search(price).ok().map(|at| &self.levels[at].1)
    }

    pub fn queue_mut(&mut self, price: Price) -> Option<&mut PriceQueue> {
        self.search(price).ok().map(|at| &mut self.levels[at].1)
    }

    pub fn entry(&mut self, price: Price) -> &mut PriceQueue {
        let at = match self.search(price) {
            Ok(at) => at,
            Err(at) => {
                self.levels.insert(at, (price, PriceQueue::new()));
                at
            }
        };
        &mut self.levels[at].1
    }

    pub fn drop_vacant(&mut self) {
        self.levels.retain(|(_, queue)| queue.holds_own_liquidity());
    }

    pub fn holds_liquidity_crossing(&self, price: Price) -> bool {
        self.levels.iter().any(|(resting, queue)| {
            is_crossed_by(self.side, *resting, price) && queue.holds_own_liquidity()
        })
    }

    fn search(&self, price: Price) -> Result<usize, usize> {
        let target = rank(self.side, price);
        self.levels
            .binary_search_by(|(probe, _)| rank(self.side, *probe).cmp(&target))
    }
}

fn is_crossed_by(side: Side, resting: Price, price: Price) -> bool {
    match side {
        Side::Buy => resting.0 >= price.0,
        Side::Sell => resting.0 <= price.0,
    }
}

fn rank(side: Side, price: Price) -> i64 {
    match side {
        Side::Buy => -price.0,
        Side::Sell => price.0,
    }
}
