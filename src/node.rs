use std::cmp::Ordering;
use std::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;
use collate::{Collate, Overlap, Overlaps};
use destream::{de, en};
use futures::TryFutureExt;
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
                Overlap::Less => hi = mid,
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

struct NodeVisitor<C, N> {
    context: C,
    phantom: PhantomData<N>,
}

impl<C, N> NodeVisitor<C, N> {
    fn new(context: C) -> Self {
        Self {
            context,
            phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<N> de::Visitor for NodeVisitor<N::Context, N>
where
    N: de::FromStream,
{
    type Value = Node<N>;

    fn expecting() -> &'static str {
        "a B+Tree node"
    }

    async fn visit_seq<A: de::SeqAccess>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let leaf = seq.expect_next::<bool>(()).await?;

        if leaf {
            seq.expect_next(self.context).map_ok(Node::Leaf).await
        } else {
            let bounds = seq.expect_next(self.context).await?;
            let children = seq.expect_next(()).await?;
            Ok(Node::Index(bounds, children))
        }
    }
}

#[async_trait]
impl<N> de::FromStream for Node<N>
where
    N: de::FromStream,
{
    type Context = N::Context;

    async fn from_stream<D: de::Decoder>(
        cxt: Self::Context,
        decoder: &mut D,
    ) -> Result<Self, D::Error> {
        decoder.decode_seq(NodeVisitor::new(cxt)).await
    }
}

impl<'en, N: 'en> en::IntoStream<'en> for Node<N>
where
    N: en::IntoStream<'en>,
{
    fn into_stream<E: en::Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Node::Leaf(keys) => (true, keys).into_stream(encoder),
            Node::Index(bounds, children) => (false, bounds, children).into_stream(encoder),
        }
    }
}

impl<'en, N: 'en> en::ToStream<'en> for Node<N>
where
    N: en::ToStream<'en>,
{
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        use en::IntoStream;

        match self {
            Node::Leaf(keys) => (true, keys).into_stream(encoder),
            Node::Index(bounds, children) => (false, bounds, children).into_stream(encoder),
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
