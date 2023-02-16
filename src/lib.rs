//! A persistent B+ tree using [`freqfs`].
//!
//! See the `examples` directory for usage examples.

use std::pin::Pin;
use std::string::ToString;
use std::sync::Arc;
use std::{fmt, io};

use freqfs::{DirLock, DirReadGuard, DirWriteGuard, FileLoad};
use futures::future::{Future, FutureExt};
use futures::stream::{self, Stream};
use safecast::AsType;
use uuid::Uuid;

mod collate;
pub mod range;

use crate::collate::{Collate, CollateRange};
use crate::range::Range;

const ROOT: Uuid = Uuid::from_fields(0, 0, 0, &[0u8; 8]);

type Key<V> = Vec<V>;

pub enum Node<V> {
    Index(Vec<Key<V>>, Vec<Uuid>),
    Leaf(Vec<Key<V>>),
}

/// The result of a B+ tree operation
pub type Result<T> = std::result::Result<T, io::Error>;

/// The schema of a B+ tree
pub trait Schema: Eq + fmt::Debug {
    type Error: std::error::Error;
    type Value: Clone + Eq;

    /// Get the maximum size in bytes of a leaf node in a B+ tree with this [`Schema`].
    fn block_size(&self) -> usize;

    /// Get the order of the nodes in a B+ tree with this [`Schema`].
    fn order(&self) -> usize;

    /// Return a validated version of the given `key`, or a validation error.
    fn validate(&self, key: Key<Self::Value>)
        -> std::result::Result<Key<Self::Value>, Self::Error>;
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
    pub fn create(schema: S, collator: C, dir: DirLock<FE>) -> Result<Self> {
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
    pub fn load(schema: S, collator: C, dir: DirLock<FE>) -> Result<Self> {
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
            .map(|dir| BTreeReadGuard {
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
            .map(|dir| BTreeWriteGuard {
                schema: self.schema.clone(),
                collator: self.collator.clone(),
                dir,
            })
            .await
    }
}

/// A read-only view of a B+ tree
pub struct BTreeReadGuard<S, C, FE> {
    schema: Arc<S>,
    collator: Arc<C>,
    dir: DirReadGuard<FE>,
}

impl<S, C, FE> BTreeReadGuard<S, C, FE>
where
    S: Schema,
    FE: FileLoad + AsType<Node<S::Value>>,
{
    /// Return `true` if this B+ tree contains the given `key`.
    pub async fn contains(&self, _key: Key<S::Value>) -> Result<bool> {
        todo!()
    }

    /// Count how many keys lie within the given `range` of this B+ tree.
    pub async fn count(&self, _range: Range<S::Value>) -> Result<u64> {
        todo!()
    }

    /// Return `true` if the given `range` of this B+ tree contains no keys.
    pub async fn is_empty(&self, _range: Range<S::Value>) -> Result<bool> {
        todo!()
    }

    /// Read all the keys in the given `range` of this B+ tree.
    pub async fn to_stream(
        &self,
        _range: Range<S::Value>,
    ) -> impl Stream<Item = Result<Key<S::Value>>> {
        // TODO
        stream::empty()
    }
}

/// A mutable view of a B+ tree
pub struct BTreeWriteGuard<S, C, FE> {
    schema: Arc<S>,
    collator: Arc<C>,
    dir: DirWriteGuard<FE>,
}

impl<S, C, FE> BTreeWriteGuard<S, C, FE>
where
    S: Schema,
    C: Collate<Value = S::Value>,
    FE: FileLoad + AsType<Node<S::Value>>,
{
    /// Delete the given `range` from this B+ tree.
    pub async fn delete(&self, _range: Key<S::Value>) -> Result<bool> {
        todo!()
    }

    /// Insert the given `key` into this B+ tree.
    pub async fn insert(&mut self, key: Key<S::Value>) -> Result<bool> {
        let mut root = self.dir.get_file(&ROOT).expect("root").write().await?;

        let new_root = match &mut *root {
            Node::Leaf(keys) => {
                let i = self.collator.bisect_left(&keys, &key);

                if i < keys.len() && keys[i] == key {
                    // no-op
                    return Ok(false);
                }

                keys.insert(i, key);

                if keys.len() > self.schema.order() {
                    let size = self.schema.block_size() / 2;
                    let right: Vec<_> = keys.drain(div_ceil(self.schema.order(), 2)..).collect();
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

                let mut child = self
                    .dir
                    .get_file(&children[i])
                    .expect("node")
                    .write()
                    .await?;

                let left = insert(
                    &mut self.dir,
                    &*self.schema,
                    &*self.collator,
                    &mut child,
                    key,
                )
                .await?;

                match left {
                    Insert::None => return Ok(false),
                    Insert::Right => {}
                    Insert::Left(key) => {
                        bounds[i] = key;
                    }
                    Insert::LeafOverflow(bound, child_id) => {
                        bounds.insert(i + 1, bound);
                        children.insert(i + 1, child_id);
                    }
                    Insert::IndexOverflow(_bounds, _children) => {
                        unimplemented!("split an index node")
                    }
                }

                None
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
    pub async fn merge(&self, other: BTreeReadGuard<S, C, FE>) -> Result<()> {
        if self.collator != other.collator {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "B+Tree to merge must have the same collation",
            ));
        }

        if self.schema != other.schema {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "cannot merge a B+Tree with schema {:?} into one with schema {:?}",
                    other.schema, self.schema
                ),
            ));
        }

        todo!()
    }
}

enum Insert<V> {
    None,
    Left(Key<V>),
    Right,
    LeafOverflow(Key<V>, Uuid),
    IndexOverflow(Vec<Key<V>>, Vec<Uuid>),
}

#[inline]
fn insert<'a, FE, S, C, V>(
    dir: &'a mut DirWriteGuard<FE>,
    schema: &'a S,
    collator: &'a C,
    node: &'a mut Node<V>,
    key: Key<V>,
) -> Pin<Box<dyn Future<Output = Result<Insert<V>>> + 'a>>
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
                debug_assert!(children.len() < schema.order());

                unimplemented!("insert into an index node");
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
