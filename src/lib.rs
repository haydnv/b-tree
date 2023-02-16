//! A persistent B+ tree using [`freqfs`].
//!
//! See the `examples` directory for usage examples.

use std::{fmt, io};

mod collate;
mod range;
mod tree;

pub use range::Range;
pub use tree::{BTree, BTreeLock, BTreeReadGuard, BTreeWriteGuard, Node};

/// The schema of a B+ tree
pub trait Schema: Eq + fmt::Debug {
    type Error: std::error::Error + From<io::Error>;
    type Value: Clone + Eq + fmt::Debug + 'static;

    /// Get the maximum size in bytes of a leaf node in a B+ tree with this [`Schema`].
    fn block_size(&self) -> usize;

    /// Get the order of the nodes in a B+ tree with this [`Schema`].
    fn order(&self) -> usize;

    /// Return a validated version of the given `key`, or a validation error.
    fn validate(&self, key: Vec<Self::Value>) -> Result<Vec<Self::Value>, Self::Error>;
}
