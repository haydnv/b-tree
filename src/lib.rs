//! A persistent B+ tree using [`freqfs`].
//!
//! See the `examples` directory for usage examples.

use std::io;
use std::string::ToString;
use std::sync::Arc;

use freqfs::{DirLock, DirReadGuard, DirWriteGuard, FileLoad};
use futures::stream::{self, Stream};
use futures::TryStream;
use safecast::AsType;
use uuid::Uuid;

mod collate;
pub mod range;

use range::Range;

const ROOT: &str = "root";

pub enum Node<V> {
    Index(Vec<Uuid>),
    Leaf(Vec<V>),
}

/// The result of a B+ tree operation
pub type Result<T> = std::result::Result<T, io::Error>;

/// The schema of a B+ tree
pub trait Schema {
    type Error: std::error::Error;
    type Value;

    /// Get the maximum size in bytes of a leaf node in a B+ tree with this [`Schema`].
    fn block_size(&self) -> usize;

    /// Get the order of the nodes in a B+ tree with this [`Schema`].
    fn order(&self) -> usize;

    /// Return a validated version of the given `key`, or a validation error.
    fn validate(&self, key: Vec<Self::Value>)
        -> std::result::Result<Vec<Self::Value>, Self::Error>;
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

        if dir.try_read()?.is_empty() {
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

    /// Borrow the schema of this B+ tree.
    pub fn schema(&self) -> &S {
        &self.schema
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
    /// Borrow the schema of the source B+ tree.
    pub fn schema(&self) -> &S {
        &self.schema
    }

    /// Return `true` if this B+ tree contains the given `key`.
    pub async fn contains(&self, _key: Vec<S::Value>) -> Result<bool> {
        todo!()
    }

    /// Count how many keys lie within the given `range` of this B+ tree.
    pub async fn count(&self, _range: Range<S::Value>) -> Result<u64> {
        todo!()
    }

    /// Return `true` if the given `range` of this B+ tree contains no keys.
    pub async fn is_empty(&self, _range: Range<S::Value>) -> Result<u64> {
        todo!()
    }

    /// Read all the keys in the given `range` of this B+ tree.
    pub async fn to_stream(
        &self,
        _range: Range<S::Value>,
    ) -> impl Stream<Item = Result<Vec<S::Value>>> {
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
    FE: FileLoad + AsType<Node<S::Value>>,
{
    /// Delete the given `key` from this B+ tree.
    pub async fn delete(&self, _key: Vec<S::Value>) -> Result<bool> {
        todo!()
    }

    /// Insert the given `key` into this B+ tree.
    pub async fn insert(&self, _key: Vec<S::Value>) -> Result<bool> {
        todo!()
    }

    /// Insert all of the given `keys` into this B+ tree.
    ///
    /// The provided keys will be collated prior to insertion.
    pub async fn insert_all<Keys: TryStream<Item = Vec<S::Value>>>(
        &self,
        _keys: Keys,
    ) -> Result<()> {
        todo!()
    }

    /// Merge the keys in the `other` B+ tree range into this one.
    ///
    /// The source B+ tree **must** have an identical schema and collation.
    pub async fn merge(&self, _other: BTreeReadGuard<S, C, FE>) {
        todo!()
    }
}
