//! A persistent B+ tree using [`freqfs`]

use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::string::ToString;
use std::sync::Arc;

use freqfs::{DirLock, FileLoad};
use futures::stream::{self, Stream};
use safecast::AsType;
use uuid::Uuid;

mod collate;
pub mod range;

use range::Range;

const ROOT: &str = "root";

type Node = Vec<Uuid>;
type Leaf<K> = Vec<K>;

/// The result of a [`BTree`] operation
pub type Result<T> = std::result::Result<T, io::Error>;

/// The schema of a [`BTree`]
pub trait Schema<K> {}

/// A persistent B+ tree
pub struct BTree<K, S, C, FE> {
    dir: DirLock<FE>,
    schema: Arc<S>,
    collator: Arc<C>,
    key: PhantomData<K>,
}

impl<K, S, C, FE> Clone for BTree<K, S, C, FE> {
    fn clone(&self) -> Self {
        Self {
            dir: self.dir.clone(),
            schema: self.schema.clone(),
            collator: self.collator.clone(),
            key: PhantomData,
        }
    }
}

impl<K, S, C, FE> BTree<K, S, C, FE>
where
    K: Eq + Ord + fmt::Debug,
    S: Schema<K>,
    FE: FileLoad + AsType<Node> + AsType<Leaf<K>>,
{
    /// Create a new [`BTree`] in `dir` with the given `schema` and `collator`
    pub fn create(dir: DirLock<FE>, schema: S, collator: C) -> Result<Self> {
        let mut nodes = dir.try_write()?;

        if dir.try_read()?.is_empty() {
            nodes.create_file::<Leaf<K>>(ROOT.to_string(), Leaf::new(), 0)?;

            Ok(Self {
                dir,
                schema: Arc::new(schema),
                collator: Arc::new(collator),
                key: PhantomData,
            })
        } else {
            Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "creating a new B+ tree requires an empty file",
            ))
        }
    }

    /// Load a [`BTree`] with the given `schema` and `collator` from `dir`
    pub fn load(dir: DirLock<FE>, schema: S, collator: C) -> Result<Self> {
        let nodes = dir.try_read()?;

        if nodes.contains(ROOT) {
            Ok(Self {
                dir,
                schema: Arc::new(schema),
                collator: Arc::new(collator),
                key: PhantomData,
            })
        } else {
            Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "cannot load a B+ tree from an empty file",
            ))
        }
    }
}

impl<K, S, C, FE> BTree<K, S, C, FE> {
    pub async fn count(&self, _range: Range<K>) -> Result<u64> {
        todo!()
    }

    pub async fn delete(&self, _key: Vec<K>) -> Result<bool> {
        todo!()
    }

    pub async fn insert(&self, _key: Vec<K>) -> Result<bool> {
        todo!()
    }

    pub async fn merge<Keys: Stream<Item = Result<K>>>(&self, _keys: Keys) {
        todo!()
    }

    pub async fn to_stream(&self, _range: Range<K>) -> impl Stream<Item = Result<K>> {
        // TODO
        stream::empty()
    }
}
