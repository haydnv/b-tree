//! A persistent B+ tree using [`freqfs`].
//!
//! See the `examples` directory for usage examples.

use std::string::ToString;
use std::sync::Arc;
use std::{fmt, io};

use freqfs::{DirLock, DirReadGuard, DirWriteGuard, FileLoad};
use futures::future::FutureExt;
use futures::stream::{self, Stream};
use safecast::AsType;
use uuid::Uuid;

mod collate;
pub mod range;

use crate::collate::{Collate, CollateRange};
use crate::range::Range;

const ROOT: &str = "root";

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
            nodes.create_file::<Node<S::Value>>(
                ROOT.to_string(),
                Node::Leaf(vec![]),
                schema.block_size(),
            )?;

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

        if let Some(root) = nodes.get_file(ROOT) {
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
        let mut root = self.dir.get_file(ROOT).expect("root").write().await?;

        let new_root = match &mut *root {
            Node::Leaf(keys) => {
                let i = self.collator.bisect_left(&keys, &key);

                if i < keys.len() && keys[i] == key {
                    // no-op
                    return Ok(false);
                }

                keys.insert(i, key);

                if keys.len() == (2 * self.schema.order()) - 1 {
                    let size = self.schema.block_size() / 2;
                    let right: Vec<_> = keys.drain(self.schema.order()..).collect();
                    let left: Vec<_> = keys.drain(..).collect();

                    let left_key = left[0].clone();
                    let (left, _) = self.dir.create_file_unique(Node::Leaf(left), size)?;

                    let right_key = right[0].clone();
                    let (right, _) = self.dir.create_file_unique(Node::Leaf(right), size)?;

                    Some(Node::Index(vec![left_key, right_key], vec![left, right]))
                } else {
                    debug_assert!(
                        keys.len() < self.schema.order() * 2,
                        "root node is a leaf with {} keys",
                        keys.len()
                    );

                    None
                }
            }
            Node::Index(_bounds, _children) => {
                unimplemented!("insert into index node")
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
