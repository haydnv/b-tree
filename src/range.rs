use std::cmp::Ordering::*;
use std::fmt;
use std::ops::{Bound, Range as Bounds};

use collate::Collate;

/// A range used to select a slice of a `BTree`
#[derive(Clone, Eq, PartialEq)]
pub struct Range<K> {
    prefix: Vec<K>,
    start: Bound<K>,
    end: Bound<K>,
}

impl<K> Default for Range<K> {
    fn default() -> Self {
        Self {
            prefix: vec![],
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
}
impl<K: PartialEq> Range<K> {
    /// Construct a new [`Range`] with the given `prefix`.
    pub fn new(prefix: Vec<K>, range: Bounds<K>) -> Self {
        let Bounds { start, end } = range;

        Self {
            prefix,
            start: Bound::Included(start),
            end: Bound::Excluded(end),
        }
    }

    /// Construct a new [`Range`] with only the given `prefix`.
    pub fn with_prefix(prefix: Vec<K>) -> Self {
        Self {
            prefix,
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }

    /// Return `true` if the `other` [`Range`] lies entirely within this one.
    pub fn contains<C: Collate<Value = K>>(&self, other: &Self, collator: &C) -> bool {
        if other.prefix.len() < self.prefix.len() {
            return false;
        }

        if &other.prefix[..self.prefix.len()] != &self.prefix[..] {
            return false;
        }

        if other.prefix.len() == self.prefix.len() {
            match &self.start {
                Bound::Unbounded => {}
                Bound::Included(outer) => match &other.start {
                    Bound::Unbounded => return false,
                    Bound::Included(inner) => {
                        if collator.compare(inner, outer) == Less {
                            return false;
                        }
                    }
                    Bound::Excluded(inner) => {
                        if collator.compare(inner, outer) != Greater {
                            return false;
                        }
                    }
                },
                Bound::Excluded(outer) => match &other.start {
                    Bound::Unbounded => return false,
                    Bound::Included(inner) => {
                        if collator.compare(inner, outer) != Greater {
                            return false;
                        }
                    }
                    Bound::Excluded(inner) => {
                        if collator.compare(inner, outer) == Less {
                            return false;
                        }
                    }
                },
            }
        } else {
            let value = &other.prefix[self.prefix.len()];

            match &self.start {
                Bound::Unbounded => {}
                Bound::Included(outer) => {
                    if collator.compare(value, outer) == Less {
                        return false;
                    }
                }
                Bound::Excluded(outer) => {
                    if collator.compare(value, outer) != Greater {
                        return false;
                    }
                }
            }

            match &self.end {
                Bound::Unbounded => {}
                Bound::Included(outer) => {
                    if collator.compare(value, outer) == Greater {
                        return false;
                    }
                }
                Bound::Excluded(outer) => {
                    if collator.compare(value, outer) != Less {
                        return false;
                    }
                }
            }
        }

        true
    }

    /// Return `true` if this [`Range`] is has only a prefix.
    pub fn has_bounds(&self) -> bool {
        match (&self.start, &self.end) {
            (Bound::Unbounded, Bound::Unbounded) => false,
            _ => true,
        }
    }

    /// Deconstruct this [`Range`] into its prefix and its start and end [`Bound`]s.
    pub fn into_inner(self) -> (Vec<K>, (Bound<K>, Bound<K>)) {
        (self.prefix, (self.start, self.end))
    }

    /// Return the length of this [`Range`].
    pub fn len(&self) -> usize {
        let len = self.prefix().len();
        match (&self.start, &self.end) {
            (Bound::Unbounded, Bound::Unbounded) => len,
            _ => len + 1,
        }
    }

    /// Borrow the prefix of this [`Range`].
    pub fn prefix(&self) -> &[K] {
        &self.prefix
    }

    /// Borrow the starting [`Bound`] of the last item in this range.
    pub fn start(&self) -> &Bound<K> {
        &self.start
    }

    /// Borrow the ending [`Bound`] of the last item in this range.
    pub fn end(&self) -> &Bound<K> {
        &self.end
    }
}

impl<P, K> From<(P, Bounds<K>)> for Range<K>
where
    Vec<K>: From<P>,
{
    fn from(tuple: (P, Bounds<K>)) -> Self {
        let (prefix, suffix) = tuple;
        let Bounds { start, end } = suffix;

        Self {
            prefix: prefix.into(),
            start: Bound::Included(start),
            end: Bound::Excluded(end),
        }
    }
}

impl<K: fmt::Debug> fmt::Debug for Range<K> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "range ")?;

        match (&self.start, &self.end) {
            (Bound::Excluded(l), Bound::Unbounded) => write!(f, "[{:?},)", l),
            (Bound::Excluded(l), Bound::Excluded(r)) => write!(f, "[{:?},{:?}]", l, r),
            (Bound::Excluded(l), Bound::Included(r)) => write!(f, "[{:?},{:?})", l, r),
            (Bound::Included(l), Bound::Unbounded) => write!(f, "({:?},)", l),
            (Bound::Included(l), Bound::Excluded(r)) => write!(f, "({:?},{:?}]", l, r),
            (Bound::Included(l), Bound::Included(r)) => write!(f, "({:?},{:?})", l, r),
            (Bound::Unbounded, Bound::Unbounded) => write!(f, "()"),
            (Bound::Unbounded, Bound::Excluded(r)) => write!(f, "(,{:?}]", r),
            (Bound::Unbounded, Bound::Included(r)) => write!(f, "(,{:?})", r),
        }?;

        write!(f, " with prefix {:?}", self.prefix)
    }
}