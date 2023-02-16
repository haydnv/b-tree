//! A persistent B+ tree using [`freqfs`].
//!
//! See the `examples` directory for usage examples.

use std::cmp::Ordering;
use std::ops::Deref;
use std::pin::Pin;
use std::string::ToString;
use std::sync::Arc;
use std::{fmt, io};

use freqfs::{Dir, DirLock, DirReadGuard, DirWriteGuard, FileLoad, FileReadGuard};
use futures::future::{self, Future, FutureExt};
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use futures::try_join;
use futures::TryFutureExt;
use safecast::AsType;
use uuid::Uuid;

mod collate;
mod range;

use crate::collate::{Collate, CollateRange};
pub use crate::range::Range;

/// A read guard acquired on a [`BTreeLock`]
pub type BTreeReadGuard<S, C, FE> = BTree<S, C, DirReadGuard<FE>>;

/// A write guard acquired on a [`BTreeLock`]
pub type BTreeWriteGuard<S, C, FE> = BTree<S, C, DirWriteGuard<FE>>;

type Key<V> = Vec<V>;

const ROOT: Uuid = Uuid::from_fields(0, 0, 0, &[0u8; 8]);

pub enum Node<V> {
    Index(Vec<Key<V>>, Vec<Uuid>),
    Leaf(Vec<Key<V>>),
}

/// The schema of a B+ tree
pub trait Schema: Eq + fmt::Debug {
    type Error: std::error::Error + From<io::Error>;
    type Value: Clone + Eq + fmt::Debug;

    /// Get the maximum size in bytes of a leaf node in a B+ tree with this [`Schema`].
    fn block_size(&self) -> usize;

    /// Get the order of the nodes in a B+ tree with this [`Schema`].
    fn order(&self) -> usize;

    /// Return a validated version of the given `key`, or a validation error.
    fn validate(&self, key: Key<Self::Value>) -> Result<Key<Self::Value>, Self::Error>;
}

/// A lock to synchronize access to a persistent B+ tree
pub struct BTreeLock<S, C, FE> {
    schema: Arc<S>,
    collator: Arc<C>,
    dir: DirLock<FE>,
}

impl<S, C, FE> Clone for BTreeLock<S, C, FE> {
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            collator: self.collator.clone(),
            dir: self.dir.clone(),
        }
    }
}

impl<S, C, FE> BTreeLock<S, C, FE> {
    /// Borrow the schema of the source B+ tree.
    pub fn schema(&self) -> &S {
        &self.schema
    }
}

impl<S, C, FE> BTreeLock<S, C, FE>
where
    S: Schema,
    FE: FileLoad + AsType<Node<S::Value>>,
{
    fn new(schema: S, collator: C, dir: DirLock<FE>) -> Self {
        Self {
            schema: Arc::new(schema),
            collator: Arc::new(collator),
            dir,
        }
    }

    /// Create a new [`BTreeLock`] in `dir` with the given `collator`
    pub fn create(schema: S, collator: C, dir: DirLock<FE>) -> Result<Self, io::Error> {
        let mut nodes = dir.try_write()?;

        if nodes.is_empty() {
            nodes.create_file::<Node<S::Value>>(ROOT.to_string(), Node::Leaf(vec![]), 0)?;

            Ok(Self::new(schema, collator, dir))
        } else {
            Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "creating a new B+ tree requires an empty file",
            ))
        }
    }

    /// Load a [`BTreeLock`] with the given `schema` and `collator` from `dir`
    pub fn load(schema: S, collator: C, dir: DirLock<FE>) -> Result<Self, io::Error> {
        let nodes = dir.try_read()?;

        if let Some(root) = nodes.get_file(&ROOT) {
            let _root = root.try_read()?;
            return Ok(Self::new(schema, collator, dir));
        }

        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "B+ tree is missing a root node",
        ))
    }
}

impl<S, C, FE> BTreeLock<S, C, FE>
where
    FE: FileLoad,
{
    /// Lock this B+ tree for reading
    pub async fn read(&self) -> BTreeReadGuard<S, C, FE> {
        self.dir
            .read()
            .map(|dir| BTree {
                schema: self.schema.clone(),
                collator: self.collator.clone(),
                dir,
            })
            .await
    }

    /// Lock this B+ tree for writing
    pub async fn write(&self) -> BTreeWriteGuard<S, C, FE> {
        self.dir
            .write()
            .map(|dir| BTree {
                schema: self.schema.clone(),
                collator: self.collator.clone(),
                dir,
            })
            .await
    }
}

/// A B+ tree
pub struct BTree<S, C, D> {
    schema: Arc<S>,
    collator: Arc<C>,
    dir: D,
}

impl<S, C, FE, G> BTree<S, C, G>
where
    S: Schema,
    C: Collate<Value = S::Value>,
    FE: FileLoad + AsType<Node<S::Value>>,
    G: Deref<Target = Dir<FE>>,
{
    /// Return `true` if this B+ tree contains the given `key`.
    pub async fn contains(&self, key: Key<S::Value>) -> Result<bool, S::Error> {
        let key = self.schema.validate(key)?;
        let mut node = self.dir.read_file(&ROOT).await?;

        loop {
            match &*node {
                Node::Leaf(keys) => {
                    let i = self.collator.bisect_left(&keys, &key);

                    break Ok(match keys.get(i) {
                        Some(present) => present == &key,
                        _ => false,
                    });
                }
                Node::Index(bounds, children) => {
                    let i = match self.collator.bisect_left(&bounds, &key) {
                        i if i == children.len() => i - 1,
                        i => i,
                    };

                    node = self.dir.read_file(&children[i]).await?;
                }
            }
        }
    }

    /// Count how many keys lie within the given `range` of this B+ tree.
    pub async fn count(&self, range: Range<S::Value>) -> Result<u64, S::Error> {
        let root = self.dir.read_file(&ROOT).await?;
        count(&self.dir, &*self.collator, &range, root)
            .map_err(S::Error::from)
            .await
    }

    /// Return `true` if the given `range` of this B+ tree contains no keys.
    pub async fn is_empty(&self, _range: Range<S::Value>) -> Result<bool, S::Error> {
        todo!()
    }

    /// Read all the keys in the given `range` of this B+ tree.
    pub async fn to_stream(
        &self,
        _range: Range<S::Value>,
    ) -> impl Stream<Item = Result<Key<S::Value>, S::Error>> {
        // TODO
        stream::empty()
    }
}

fn count<'a, C, V, FE>(
    dir: &'a Dir<FE>,
    collator: &'a C,
    range: &'a Range<V>,
    node: FileReadGuard<FE, Node<V>>,
) -> Pin<Box<dyn Future<Output = Result<u64, io::Error>> + 'a>>
where
    C: Collate<Value = V>,
    V: PartialEq + fmt::Debug + 'a,
    FE: FileLoad + AsType<Node<V>>,
{
    Box::pin(async move {
        match &*node {
            Node::Leaf(keys) if range == &Range::default() => Ok(keys.len() as u64),
            Node::Leaf(keys) => {
                let (l, r) = collator.bisect(&keys, &range);

                if l == r {
                    let cmp = collator.compare_range(&keys[l], range);
                    if cmp == Ordering::Equal {
                        Ok(1)
                    } else {
                        Ok(0)
                    }
                } else {
                    Ok((r - l) as u64)
                }
            }
            Node::Index(bounds, children) => {
                let (l, r) = collator.bisect(&bounds, &range);

                if l == children.len() {
                    let node = dir.read_file(children.last().expect("last")).await?;
                    count(dir, collator, range, node).await
                } else if l == r {
                    let node = dir.read_file(&children[l]).await?;
                    count(dir, collator, range, node).await
                } else if l + 1 == r {
                    let (left, right) =
                        try_join!(dir.read_file(&children[l]), dir.read_file(&children[r]))?;

                    let (l_count, r_count) = try_join!(
                        count(dir, collator, range, left),
                        count(dir, collator, range, right)
                    )?;

                    Ok(l_count + r_count)
                } else {
                    let r = if r == children.len() { r - 1 } else { r };

                    let left = dir
                        .read_file(&children[l])
                        .and_then(|node| count(dir, collator, range, node));

                    let default_range = Range::default();

                    let middle = stream::iter(&children[(l + 1)..r])
                        .then(|node_id| dir.read_file(node_id))
                        .map_ok(|node| count(dir, collator, &default_range, node))
                        .try_buffer_unordered(num_cpus::get())
                        .try_fold(0, |sum, count| future::ready(Ok(sum + count)));

                    let right = dir
                        .read_file(&children[r])
                        .and_then(|node| count(dir, collator, range, node));

                    let (left, middle, right) = try_join!(left, middle, right)?;
                    Ok(left + middle + right)
                }
            }
        }
    })
}

impl<S, C, FE> BTree<S, C, DirWriteGuard<FE>>
where
    S: Schema,
    C: Collate<Value = S::Value>,
    FE: FileLoad + AsType<Node<S::Value>>,
{
    /// Delete the given `range` from this B+ tree.
    pub async fn delete(&mut self, _range: Key<S::Value>) -> Result<bool, S::Error> {
        todo!()
    }

    /// Insert the given `key` into this B+ tree.
    pub async fn insert(&mut self, key: Key<S::Value>) -> Result<bool, S::Error> {
        let key = self.schema.validate(key)?;

        let order = self.schema.order();
        let mut root = self.dir.write_file(&ROOT).await?;

        let new_root = match &mut *root {
            Node::Leaf(keys) => {
                let i = self.collator.bisect_left(&keys, &key);

                if i < keys.len() && keys[i] == key {
                    // no-op
                    return Ok(false);
                }

                keys.insert(i, key);

                debug_assert!(self.collator.is_sorted(&keys));

                if keys.len() > order {
                    let size = self.schema.block_size() / 2;
                    let right: Vec<_> = keys.drain(div_ceil(order, 2)..).collect();
                    let left: Vec<_> = keys.drain(..).collect();

                    let left_key = left[0].clone();
                    let (left, _) = self.dir.create_file_unique(Node::Leaf(left), size)?;

                    let right_key = right[0].clone();
                    let (right, _) = self.dir.create_file_unique(Node::Leaf(right), size)?;

                    Some(Node::Index(vec![left_key, right_key], vec![left, right]))
                } else {
                    None
                }
            }
            Node::Index(bounds, children) => {
                debug_assert_eq!(bounds.len(), children.len());

                let i = match self.collator.bisect_left(&bounds, &key) {
                    0 => 0,
                    i => i - 1,
                };

                let mut child = self.dir.write_file(&children[i]).await?;

                let result = insert(
                    &mut self.dir,
                    &*self.schema,
                    &*self.collator,
                    &mut child,
                    key,
                )
                .await?;

                match result {
                    Insert::None => return Ok(false),
                    Insert::Right => {}
                    Insert::Left(key) => {
                        bounds[i] = key;
                    }
                    Insert::LeafOverflow(bound, child_id) => {
                        bounds.insert(i + 1, bound);
                        children.insert(i + 1, child_id);
                    }
                    Insert::IndexOverflow(bound, child_id) => {
                        bounds.insert(i + 1, bound);
                        children.insert(i + 1, child_id);
                    }
                }

                debug_assert!(self.collator.is_sorted(&bounds));
                debug_assert_eq!(bounds.len(), children.len());

                if bounds.len() > order {
                    let size = self.schema.block_size() / 2;
                    let right_bounds: Vec<_> = bounds.drain(div_ceil(order, 2)..).collect();
                    let right_children: Vec<_> = children.drain(div_ceil(order, 2)..).collect();
                    let right_bound = right_bounds[0].clone();
                    let (right_node_id, _) = self
                        .dir
                        .create_file_unique(Node::Index(right_bounds, right_children), size)?;

                    let left_bounds: Vec<_> = bounds.drain(..).collect();
                    let left_children: Vec<_> = children.drain(..).collect();
                    let left_bound = left_bounds[0].clone();
                    let (left_node_id, _) = self
                        .dir
                        .create_file_unique(Node::Index(left_bounds, left_children), size)?;

                    Some(Node::Index(
                        vec![left_bound, right_bound],
                        vec![left_node_id, right_node_id],
                    ))
                } else {
                    None
                }
            }
        };

        if let Some(new_root) = new_root {
            *root = new_root;
        }

        Ok(true)
    }

    /// Merge the keys in the `other` B+ tree range into this one.
    ///
    /// The source B+ tree **must** have an identical schema and collation.
    pub async fn merge(&mut self, other: BTree<S, C, FE>) -> Result<(), S::Error> {
        if self.collator != other.collator {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "B+Tree to merge must have the same collation",
            )
            .into());
        }

        if self.schema != other.schema {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "cannot merge a B+Tree with schema {:?} into one with schema {:?}",
                    other.schema, self.schema
                ),
            )
            .into());
        }

        todo!()
    }
}

enum Insert<V> {
    None,
    Left(Key<V>),
    Right,
    LeafOverflow(Key<V>, Uuid),
    IndexOverflow(Key<V>, Uuid),
}

fn insert<'a, FE, S, C, V>(
    dir: &'a mut Dir<FE>,
    schema: &'a S,
    collator: &'a C,
    node: &'a mut Node<V>,
    key: Key<V>,
) -> Pin<Box<dyn Future<Output = Result<Insert<V>, io::Error>> + 'a>>
where
    FE: FileLoad + AsType<Node<V>>,
    S: Schema<Value = V>,
    C: Collate<Value = V>,
    V: Clone + PartialEq + 'a,
{
    Box::pin(async move {
        match node {
            Node::Leaf(keys) => {
                let i = collator.bisect_left(&keys, &key);

                if i < keys.len() && keys[i] == key {
                    // no-op
                    return Ok(Insert::None);
                }

                keys.insert(i, key);

                debug_assert!(collator.is_sorted(&keys));

                if keys.len() > schema.order() {
                    let size = schema.block_size() / 2;
                    let new_leaf: Vec<_> = keys.drain(div_ceil(schema.order(), 2)..).collect();

                    let left_key = new_leaf[0].clone();
                    let (new_node_id, _) = dir.create_file_unique(Node::Leaf(new_leaf), size)?;

                    Ok(Insert::LeafOverflow(left_key, new_node_id))
                } else {
                    debug_assert!(keys.len() > div_ceil(schema.order(), 2));

                    if i == 0 {
                        Ok(Insert::Left(keys[0].clone()))
                    } else {
                        Ok(Insert::Right)
                    }
                }
            }
            Node::Index(bounds, children) => {
                debug_assert_eq!(bounds.len(), children.len());
                debug_assert!(children.len() > div_ceil(schema.order(), 2) - 1);
                debug_assert!(children.len() <= schema.order());

                let i = match collator.bisect_left(&bounds, &key) {
                    0 => 0,
                    i => i - 1,
                };

                let mut child = dir.write_file(&children[i]).await?;

                let result = insert(dir, schema, collator, &mut child, key).await?;

                let result = match result {
                    Insert::None => Insert::None,
                    Insert::Right => Insert::Right,
                    Insert::Left(key) => {
                        bounds[i] = key;

                        if i == 0 {
                            Insert::Left(bounds[i].clone())
                        } else {
                            Insert::Right
                        }
                    }
                    Insert::LeafOverflow(bound, child_id) => {
                        bounds.insert(i + 1, bound);
                        children.insert(i + 1, child_id);

                        if children.len() > schema.order() {
                            let new_bounds: Vec<_> =
                                bounds.drain(div_ceil(schema.order(), 2)..).collect();

                            let new_children: Vec<_> =
                                children.drain(div_ceil(schema.order(), 2)..).collect();

                            let left_bound = new_bounds[0].clone();
                            let (node_id, _) = dir.create_file_unique(
                                Node::Index(new_bounds, new_children),
                                schema.block_size() / 2,
                            )?;

                            Insert::IndexOverflow(left_bound, node_id)
                        } else {
                            Insert::Right
                        }
                    }
                    Insert::IndexOverflow(bound, child_id) => {
                        bounds.insert(i + 1, bound);
                        children.insert(i + 1, child_id);

                        if children.len() > schema.order() {
                            let new_bounds: Vec<_> =
                                bounds.drain(div_ceil(schema.order(), 2)..).collect();

                            let new_children: Vec<_> =
                                children.drain(div_ceil(schema.order(), 2)..).collect();

                            let left_bound = new_bounds[0].clone();
                            let (node_id, _) = dir.create_file_unique(
                                Node::Index(new_bounds, new_children),
                                schema.block_size() / 2,
                            )?;

                            Insert::IndexOverflow(left_bound, node_id)
                        } else {
                            Insert::Right
                        }
                    }
                };

                Ok(result)
            }
        }
    })
}

#[inline]
fn div_ceil(num: usize, denom: usize) -> usize {
    match num % denom {
        0 => num / denom,
        _ => (num / denom) + 1,
    }
}
