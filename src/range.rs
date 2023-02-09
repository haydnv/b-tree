use std::fmt;
use std::ops::{Bound, Range as Bounds};

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
impl<K> Range<K> {
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

impl<K> From<(Vec<K>, Bounds<K>)> for Range<K> {
    fn from(tuple: (Vec<K>, Bounds<K>)) -> Self {
        let (prefix, suffix) = tuple;
        let Bounds { start, end } = suffix;

        Self {
            prefix,
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
