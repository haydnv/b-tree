use std::cmp::Ordering;
use std::fmt;

use collate::{Collate, Overlap, Overlaps};
use uuid::Uuid;

use super::range::Range;
use super::Collator;
use super::Key;

/// An ordered set of keys in a [`Node`].
pub trait Block<V> {
    type Key;

    fn bisect<C>(&self, range: &Range<V>, collator: &Collator<C>) -> (usize, usize)
    where
        C: Collate<Value = V>;

    fn bisect_left<C>(&self, key: &Key<V>, collator: &Collator<C>) -> usize
    where
        C: Collate<Value = V>;

    fn bisect_right<C>(&self, key: &Key<V>, collator: &Collator<C>) -> usize
    where
        C: Collate<Value = V>;
}

impl<V: fmt::Debug> Block<V> for Vec<Key<V>> {
    type Key = Key<V>;

    fn bisect<C>(&self, range: &Range<V>, collator: &Collator<C>) -> (usize, usize)
    where
        C: Collate<Value = V>,
    {
        // handle common edge cases

        if range.is_default() {
            return (0, self.len());
        }

        if let Some(first) = self.first() {
            if range.overlaps(first, collator) == Overlap::Less {
                return (0, 0);
            }
        }

        if let Some(last) = self.last() {
            if range.overlaps(last, collator) == Overlap::Greater {
                return (self.len(), self.len());
            }
        }

        // bisect range left
        let mut lo = 0;
        let mut hi = self.len();

        while lo < hi {
            let mid = (lo + hi) >> 1;
            match range.overlaps(&self[mid], collator) {
                Overlap::Greater => lo = mid + 1,
                _ => hi = mid,
            }
        }

        let left = lo;

        // bisect range right
        let mut lo = 0;
        let mut hi = self.len();

        while lo < hi {
            let mid = (lo + hi) >> 1;
            match range.overlaps(&self[mid], collator) {
                Overlap::Less => {
                    hi = mid
                },
                _ => lo = mid + 1,
            }
        }

        let right = hi;

        // return
        (left, right)
    }

    fn bisect_left<C>(&self, key: &Key<V>, collator: &Collator<C>) -> usize
    where
        C: Collate<Value = V>,
    {
        let mut lo = 0;
        let mut hi = self.len();

        while lo < hi {
            let mid = (lo + hi) >> 1;
            match collator.cmp(&self[mid], key) {
                Ordering::Less => lo = mid + 1,
                _ => hi = mid,
            }
        }

        lo
    }

    fn bisect_right<C>(&self, key: &Key<V>, collator: &Collator<C>) -> usize
    where
        C: Collate<Value = V>,
    {
        let mut lo = 0;
        let mut hi = self.len();

        while lo < hi {
            let mid = (lo + hi) >> 1;
            match collator.cmp(&self[mid], key) {
                Ordering::Greater => hi = mid,
                _ => lo = mid + 1,
            }
        }

        hi
    }
}

/// A node in a B+Tree
pub enum Node<N> {
    Index(N, Vec<Uuid>),
    Leaf(N),
}

impl<N> Node<N> {
    /// Return `true` if this is a leaf node.
    pub fn is_leaf(&self) -> bool {
        match self {
            Self::Leaf(_) => true,
            _ => false,
        }
    }
}

#[cfg(debug_assertions)]
impl<N: fmt::Debug> fmt::Debug for Node<N> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Leaf(keys) => write!(f, "leaf node with keys {:?}", keys),
            Self::Index(bounds, children) => write!(
                f,
                "index node with bounds {:?} and children {:?}",
                bounds, children
            ),
        }
    }
}

#[cfg(not(debug_assertions))]
impl<V: fmt::Debug> fmt::Debug for Node<V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Leaf(_keys) => write!(f, "a leaf node"),
            Self::Index(_bounds, children) => {
                write!(f, "an index node with {} children", children.len())
            }
        }
    }
}
