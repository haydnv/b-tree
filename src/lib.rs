//! A persistent B+ tree using [`freqfs`].
//!
//! See the `examples` directory for usage examples.

use std::cmp::Ordering;
use std::{fmt, io};

use collate::Collate;

mod node;
mod range;
mod tree;

pub use node::{Block, Node};
pub use range::Range;
pub use tree::{BTree, BTreeLock, BTreeReadGuard, BTreeWriteGuard};

pub use collate;

/// A key in a B+Tree
pub type Key<V> = Vec<V>;

/// A collator used by a B+Tree
#[derive(Copy, Clone)]
pub struct Collator<C> {
    value: C,
}

impl<C> Collator<C> {
    /// Construct a new [`Collator`] for a B+Tree from an existing `value` collator.
    pub fn new(value: C) -> Self {
        Self { value }
    }

    /// Borrow the value collator used by this key [`Collator`].
    pub fn inner(&self) -> &C {
        &self.value
    }
}

impl<C> Collate for Collator<C>
where
    C: Collate,
{
    type Value = Key<C::Value>;

    fn cmp(&self, left: &Self::Value, right: &Self::Value) -> Ordering {
        for i in 0..Ord::min(left.len(), right.len()) {
            match self.value.cmp(&left[i], &right[i]) {
                Ordering::Equal => {}
                ord => return ord,
            }
        }

        Ordering::Equal
    }
}

impl<C> PartialEq for Collator<C>
where
    C: Collate,
{
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<C> Eq for Collator<C> where C: Collate {}

/// The schema of a B+Tree
pub trait Schema: Eq + fmt::Debug {
    type Error: std::error::Error + From<io::Error>;
    type Value: Clone + Eq + Send + Sync + fmt::Debug + 'static;

    /// Get the maximum size in bytes of a leaf node in a B+Tree with this [`Schema`].
    fn block_size(&self) -> usize;

    /// Get the number of columns in this [`Schema`].
    fn len(&self) -> usize;

    /// Get the order of the nodes in a B+Tree with this [`Schema`].
    fn order(&self) -> usize;

    /// Return a validated version of the given `key`, or a validation error.
    fn validate(&self, key: Vec<Self::Value>) -> Result<Vec<Self::Value>, Self::Error>;
}
