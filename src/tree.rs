use std::cmp::Ordering;
use std::ops::Deref;
use std::pin::Pin;
use std::string::ToString;
use std::sync::Arc;
use std::{fmt, io};

use freqfs::*;
use futures::future::{self, Future, FutureExt};
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use futures::try_join;
use futures::TryFutureExt;
use safecast::AsType;
use uuid::Uuid;

use super::collate::{Collate, CollateRange};
use super::range::Range;
use super::Schema;

/// A read guard acquired on a [`BTreeLock`]
pub type BTreeReadGuard<S, C, FE> = BTree<S, C, DirReadGuardOwned<FE>>;

/// A write guard acquired on a [`BTreeLock`]
pub type BTreeWriteGuard<S, C, FE> = BTree<S, C, DirWriteGuardOwned<FE>>;

type Key<V> = Vec<V>;

const ROOT: Uuid = Uuid::from_fields(0, 0, 0, &[0u8; 8]);

pub enum Node<V> {
    Index(Vec<Key<V>>, Vec<Uuid>),
    Leaf(Vec<Key<V>>),
}

impl<V> Node<V> {
    fn is_leaf(&self) -> bool {
        match self {
            Self::Leaf(_) => true,
            _ => false,
        }
    }
}

#[cfg(debug_assertions)]
impl<V: fmt::Debug> fmt::Debug for Node<V> {
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
            Self::Leaf(keys) => write!(f, "a leaf node with {} keys", keys.len()),
            Self::Index(bounds, children) => write!(
                f,
                "an index node with {} bounds and {} children",
                bounds.len(),
                children.len()
            ),
        }
    }
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
        let mut nodes = dir.try_write_owned()?;

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
        let nodes = dir.try_read_owned()?;

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
            .read_owned()
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
            .write_owned()
            .map(|dir| BTree {
                schema: self.schema.clone(),
                collator: self.collator.clone(),
                dir,
            })
            .await
    }
}

type IntoStream<V> = Pin<Box<dyn Stream<Item = Result<Key<V>, io::Error>>>>;

type ToStream<'a, FE, V> =
    Pin<Box<dyn Stream<Item = Result<FileReadGuardOwned<FE, [Key<V>]>, io::Error>> + 'a>>;

/// A B+ tree
pub struct BTree<S, C, D> {
    schema: Arc<S>,
    collator: Arc<C>,
    dir: D,
}

impl<S, C, FE, G> BTree<S, C, G>
where
    S: Schema,
    C: Collate<Value = S::Value> + 'static,
    FE: FileLoad + AsType<Node<S::Value>>,
    G: Deref<Target = Dir<FE>> + 'static,
    Node<S::Value>: fmt::Debug,
{
    /// Return `true` if this B+ tree contains the given `key`.
    pub async fn contains(&self, key: &Key<S::Value>) -> Result<bool, io::Error> {
        let mut node = self.dir.read_file(&ROOT).await?;

        loop {
            match &*node {
                Node::Leaf(keys) => {
                    let i = self.collator.bisect_left(&keys, key);
                    break Ok(if i < keys.len() {
                        match keys.get(i) {
                            Some(present) => present == key,
                            _ => false,
                        }
                    } else {
                        false
                    });
                }
                Node::Index(bounds, children) => {
                    let i = self.collator.bisect_right(&bounds, key);
                    if i == 0 {
                        return Ok(false);
                    } else {
                        node = self.dir.read_file(&children[i - 1]).await?;
                    }
                }
            }
        }
    }

    /// Count how many keys lie within the given `range` of this B+ tree.
    pub async fn count(&self, range: &Range<S::Value>) -> Result<u64, io::Error> {
        let root = self.dir.read_file(&ROOT).await?;
        self.count_inner(range, root).await
    }

    fn count_inner<'a>(
        &'a self,
        range: &'a Range<S::Value>,
        node: FileReadGuard<'a, Node<S::Value>>,
    ) -> Pin<Box<dyn Future<Output = Result<u64, io::Error>> + 'a>> {
        Box::pin(async move {
            match &*node {
                Node::Leaf(keys) if range == &Range::default() => Ok(keys.len() as u64),
                Node::Leaf(keys) => {
                    let (l, r) = self.collator.bisect(&keys, range);

                    if l == keys.len() {
                        Ok(0)
                    } else if l == r {
                        let cmp = self.collator.compare_range(&keys[l], range);
                        if cmp == Ordering::Equal {
                            Ok(1)
                        } else {
                            Ok(0)
                        }
                    } else {
                        Ok((r - l) as u64)
                    }
                }
                Node::Index(_bounds, children) if range == &Range::default() => {
                    stream::iter(children)
                        .then(|node_id| self.dir.read_file(node_id))
                        .map_ok(|node| self.count_inner(range, node))
                        .try_buffer_unordered(num_cpus::get())
                        .try_fold(0, |sum, count| future::ready(Ok(sum + count)))
                        .await
                }
                Node::Index(bounds, children) => {
                    let (l, r) = self.collator.bisect(&bounds, range);

                    if l == children.len() {
                        let node = self.dir.read_file(children.last().expect("last")).await?;
                        self.count_inner(range, node).await
                    } else if l == r || l + 1 == r {
                        let node = self.dir.read_file(&children[l]).await?;
                        self.count_inner(range, node).await
                    } else {
                        let left = self
                            .dir
                            .read_file(&children[l])
                            .and_then(|node| self.count_inner(range, node));

                        let default_range = Range::default();

                        let middle = stream::iter(&children[(l + 1)..(r - 1)])
                            .then(|node_id| self.dir.read_file(node_id))
                            .map_ok(|node| self.count_inner(&default_range, node))
                            .try_buffer_unordered(num_cpus::get())
                            .try_fold(0, |sum, count| future::ready(Ok(sum + count)));

                        let right = self
                            .dir
                            .read_file(&children[r - 1])
                            .and_then(|node| self.count_inner(range, node));

                        let (left, middle, right) = try_join!(left, middle, right)?;
                        Ok(left + middle + right)
                    }
                }
            }
        })
    }

    /// Return the first key in this B+Tree, if any.
    pub async fn first(&self) -> Result<Option<Key<S::Value>>, io::Error> {
        let mut node = self.dir.read_file(&ROOT).await?;

        loop {
            match &*node {
                Node::Leaf(keys) => return Ok(keys.first().cloned()),
                Node::Index(_bounds, children) => {
                    node = self.dir.read_file(&children[0]).await?;
                }
            }
        }
    }

    /// Return the last key in this B+Tree, if any.
    pub async fn last(&self) -> Result<Option<Key<S::Value>>, io::Error> {
        let mut node = self.dir.read_file(&ROOT).await?;

        loop {
            match &*node {
                Node::Leaf(keys) => return Ok(keys.last().cloned()),
                Node::Index(_bounds, children) => {
                    node = self.dir.read_file(children.last().expect("last")).await?;
                }
            }
        }
    }

    /// Return `true` if the given `range` of this B+ tree contains no keys.
    pub async fn is_empty(&self, range: &Range<S::Value>) -> Result<bool, S::Error> {
        let mut node = self.dir.read_file(&ROOT).await?;

        Ok(loop {
            match &*node {
                Node::Leaf(keys) => {
                    let (l, r) = self.collator.bisect(&keys, range);
                    break l == r;
                }
                Node::Index(bounds, children) => {
                    let (l, r) = self.collator.bisect(&bounds, range);

                    if l == children.len() {
                        node = self.dir.read_file(&children[l - 1]).await?;
                    } else if l == r {
                        node = self.dir.read_file(&children[l]).await?;
                    } else {
                        break false;
                    }
                }
            }
        })
    }

    /// Borrow all the keys in the given `range` of this B+ tree.
    pub fn to_stream<'a>(
        &'a self,
        range: &'a Range<S::Value>,
    ) -> impl Stream<Item = Result<FileReadGuardOwned<FE, [Key<S::Value>]>, io::Error>> + Sized + 'a
    {
        let range = if range == &Range::default() {
            None
        } else {
            Some(range)
        };

        self.to_stream_inner(range, ROOT)
    }

    fn to_stream_inner<'a>(&'a self, range: Option<&'a Range<S::Value>>, node_id: Uuid) -> ToStream<'a, FE, S::Value> {
        let file = self.dir.get_file(&node_id).expect("node").clone();
        let fut = file.into_read().map_ok(move |node| {
            let keys: ToStream<FE, S::Value> = {
                if node.is_leaf() {
                    let guard = FileReadGuardOwned::try_map(node, |node| match node {
                        Node::Leaf(keys) => match range {
                            None => Some(&keys[..]),
                            Some(range) => {
                                let (l, r) = self.collator.bisect(&keys, &range);

                                let slice = if l == r && l < keys.len() {
                                    let cmp = self.collator.compare_range(&keys[l], &*range);
                                    if cmp == Ordering::Equal {
                                        &keys[l..l + 1]
                                    } else {
                                        &keys[l..l]
                                    }
                                } else {
                                    &keys[l..r]
                                };

                                Some(slice)
                            }
                        },
                        Node::Index(_, _) => None,
                    })
                        .expect("leaf");

                    Box::pin(stream::once(future::ready(Ok(guard))))
                } else {
                    let children = match &*node {
                        Node::Index(bounds, children) => match range {
                            Some(range) => {
                                let (l, r) = self.collator.bisect(&bounds, &range);
                                children[l..r].to_vec()
                            }
                            None => children.to_vec(),
                        },
                        _ => unreachable!("leaf case handled above"),
                    };

                    let keys = children
                        .into_iter()
                        .map(move |node_id| self.to_stream_inner(range, node_id));

                    Box::pin(stream::iter(keys).flatten())
                }
            };

            keys
        });

        Box::pin(stream::once(fut).try_flatten())
    }

    /// Copy all the keys in the given `range` of this B+ tree.
    pub fn into_stream(
        self,
        range: Range<S::Value>,
    ) -> impl Stream<Item = Result<Key<S::Value>, io::Error>> + Sized {
        into_stream(Arc::new(self.dir), self.collator, Arc::new(range), ROOT)
    }

    #[cfg(debug_assertions)]
    pub async fn is_valid(&self) -> Result<bool, io::Error> {
        let range = Range::default();
        let count = self.count(&range).await? as usize;
        let mut contents = Vec::with_capacity(count);
        let mut stream = self.to_stream(&range);
        while let Some(leaf) = stream.try_next().await? {
            contents.extend_from_slice(&*leaf);
        }

        assert_eq!(count, contents.len());
        assert!(
            self.collator.is_sorted(&contents),
            "not sorted: {:?}",
            contents
        );

        Ok(true)
    }
}

fn into_stream<C, V, FE, G>(
    dir: Arc<G>,
    collator: Arc<C>,
    range: Arc<Range<V>>,
    node_id: Uuid,
) -> IntoStream<V>
where
    C: Collate<Value = V> + 'static,
    V: Clone + PartialEq + 'static,
    FE: FileLoad + AsType<Node<V>>,
    G: Deref<Target = Dir<FE>> + 'static,
{
    let file = dir.get_file(&node_id).expect("node").clone();
    let fut = file.into_read().map_ok(move |node| {
        let keys: IntoStream<V> = match &*node {
            Node::Leaf(keys) if *range == Range::default() => {
                let keys = stream::iter(keys.to_vec().into_iter().map(Ok));
                Box::pin(keys)
            }
            Node::Leaf(keys) => {
                let (l, r) = collator.bisect(&keys, &range);

                if l == keys.len() {
                    Box::pin(stream::empty())
                } else if l == r {
                    let cmp = collator.compare_range(&keys[l], &*range);
                    if cmp == Ordering::Equal {
                        Box::pin(stream::once(future::ready(Ok(keys[l].to_vec()))))
                    } else {
                        Box::pin(stream::empty())
                    }
                } else {
                    Box::pin(stream::iter(keys[l..r].to_vec().into_iter().map(Ok)))
                }
            }
            Node::Index(_bounds, children) if *range == Range::default() => {
                let keys = stream::iter(children.to_vec())
                    .map(move |node_id| {
                        into_stream(dir.clone(), collator.clone(), range.clone(), node_id)
                    })
                    .flatten();

                Box::pin(keys)
            }
            Node::Index(bounds, children) => {
                let (l, r) = collator.bisect(&bounds, &range);

                if l == children.len() {
                    into_stream(dir, collator, range, children[l - 1])
                } else if l == r || l + 1 == r {
                    into_stream(dir, collator, range, children[l])
                } else {
                    let r = if r == children.len() { r - 1 } else { r };

                    let left =
                        into_stream(dir.clone(), collator.clone(), range.clone(), children[l]);

                    let right = into_stream(dir.clone(), collator.clone(), range, children[r]);

                    let default_range = Arc::new(Range::default());

                    let middle = stream::iter(children[(l + 1)..r].to_vec())
                        .map(move |node_id| {
                            into_stream(
                                dir.clone(),
                                collator.clone(),
                                default_range.clone(),
                                node_id,
                            )
                        })
                        .flatten();

                    Box::pin(left.chain(middle).chain(right))
                }
            }
        };

        keys
    });

    Box::pin(stream::once(fut).try_flatten())
}

enum Insert<V> {
    None,
    Left(Key<V>),
    Right,
    OverflowLeft(Key<V>, Key<V>, Uuid),
    Overflow(Key<V>, Uuid),
}

impl<S, C, FE> BTree<S, C, DirWriteGuardOwned<FE>>
where
    S: Schema,
    C: Collate<Value = S::Value> + 'static,
    FE: FileLoad + AsType<Node<S::Value>>,
{
    /// Insert the given `key` into this B+ tree.
    pub async fn insert(&mut self, key: Key<S::Value>) -> Result<bool, S::Error> {
        let key = self.schema.validate(key)?;
        self.insert_root(key).await
    }

    async fn insert_root(&mut self, key: Key<S::Value>) -> Result<bool, S::Error> {
        let order = self.schema.order();
        let mut root = self.dir.write_file_owned(&ROOT).await?;

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
                    let mid = div_ceil(order, 2);
                    let size = self.schema.block_size() / 2;

                    let right: Vec<_> = keys.drain(mid..).collect();
                    debug_assert!(right.len() >= mid);

                    let right_key = right[0].clone();
                    let (right, _) = self.dir.create_file_unique(Node::Leaf(right), size)?;

                    let left: Vec<_> = keys.drain(..).collect();
                    debug_assert!(left.len() >= mid);

                    let left_key = left[0].clone();
                    let (left, _) = self.dir.create_file_unique(Node::Leaf(left), size)?;

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

                let mut child = self.dir.write_file_owned(&children[i]).await?;
                let result = self.insert_inner(&mut child, key).await?;

                match result {
                    Insert::None => return Ok(false),
                    Insert::Right => {}
                    Insert::Left(key) => {
                        bounds[i] = key;
                    }
                    Insert::OverflowLeft(left, middle, child_id) => {
                        bounds[i] = left;
                        bounds.insert(i + 1, middle);
                        children.insert(i + 1, child_id);
                    }
                    Insert::Overflow(bound, child_id) => {
                        bounds.insert(i + 1, bound);
                        children.insert(i + 1, child_id);
                    }
                }

                debug_assert!(self.collator.is_sorted(&bounds));
                debug_assert_eq!(bounds.len(), children.len());

                if children.len() > order {
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

    fn insert_inner<'a>(
        &'a mut self,
        node: &'a mut Node<S::Value>,
        key: Key<S::Value>,
    ) -> Pin<Box<dyn Future<Output = Result<Insert<S::Value>, io::Error>> + 'a>> {
        Box::pin(async move {
            match node {
                Node::Leaf(keys) => {
                    debug_assert!(keys.len() <= self.schema.order());
                    debug_assert!(self.collator.is_sorted(keys));

                    let i = self.collator.bisect_left(&keys, &key);

                    if i < keys.len() && keys[i] == key {
                        // no-op
                        return Ok(Insert::None);
                    }

                    keys.insert(i, key);

                    debug_assert!(self.collator.is_sorted(&keys));

                    if keys.len() > self.schema.order() {
                        let mid = div_ceil(self.schema.order(), 2);
                        let size = self.schema.block_size() / 2;
                        let new_leaf: Vec<_> = keys.drain(mid..).collect();

                        debug_assert!(new_leaf.len() >= mid);
                        debug_assert!(keys.len() >= mid);

                        let middle_key = new_leaf[0].clone();
                        let (new_node_id, _) =
                            self.dir.create_file_unique(Node::Leaf(new_leaf), size)?;

                        if i == 0 {
                            Ok(Insert::OverflowLeft(
                                keys[0].clone(),
                                middle_key,
                                new_node_id,
                            ))
                        } else {
                            Ok(Insert::Overflow(middle_key, new_node_id))
                        }
                    } else {
                        debug_assert!(keys.len() > div_ceil(self.schema.order(), 2));

                        if i == 0 {
                            Ok(Insert::Left(keys[0].clone()))
                        } else {
                            Ok(Insert::Right)
                        }
                    }
                }
                Node::Index(bounds, children) => {
                    let order = self.schema.order();
                    let size = self.schema.block_size() >> 1;

                    debug_assert_eq!(bounds.len(), children.len());
                    debug_assert!(self.collator.is_sorted(bounds));
                    debug_assert!(children.len() > div_ceil(order, 2) - 1);
                    debug_assert!(children.len() <= order);

                    let i = match self.collator.bisect_left(&bounds, &key) {
                        0 => 0,
                        i => i - 1,
                    };

                    let mut child = self.dir.write_file_owned(&children[i]).await?;

                    match self.insert_inner(&mut child, key).await? {
                        Insert::None => return Ok(Insert::None),
                        Insert::Right => return Ok(Insert::Right),
                        Insert::Left(key) => {
                            bounds[i] = key;

                            return if i == 0 {
                                Ok(Insert::Left(bounds[i].clone()))
                            } else {
                                Ok(Insert::Right)
                            };
                        }
                        Insert::OverflowLeft(left, middle, child_id) => {
                            bounds[i] = left;
                            bounds.insert(i + 1, middle);
                            children.insert(i + 1, child_id);
                        }
                        Insert::Overflow(bound, child_id) => {
                            bounds.insert(i + 1, bound);
                            children.insert(i + 1, child_id);
                        }
                    }

                    debug_assert!(self.collator.is_sorted(bounds));

                    if children.len() > order {
                        let mid = div_ceil(order, 2);

                        let new_bounds: Vec<_> = bounds.drain(mid..).collect();
                        let new_children: Vec<_> = children.drain(mid..).collect();

                        let left_bound = new_bounds[0].clone();
                        let node = Node::Index(new_bounds, new_children);
                        let (node_id, _) = self.dir.create_file_unique(node, size)?;

                        if i == 0 {
                            Ok(Insert::OverflowLeft(
                                bounds[0].to_vec(),
                                left_bound,
                                node_id,
                            ))
                        } else {
                            Ok(Insert::Overflow(left_bound, node_id))
                        }
                    } else if i == 0 {
                        Ok(Insert::Left(bounds[0].clone()))
                    } else {
                        Ok(Insert::Right)
                    }
                }
            }
        })
    }

    /// Merge the keys in the `other` B+ tree range into this one.
    ///
    /// The source B+ tree **must** have an identical schema and collation.
    pub async fn merge(&mut self, other: BTreeReadGuard<S, C, FE>) -> Result<(), S::Error> {
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

        let mut keys = other.into_stream(Range::default());
        while let Some(key) = keys.try_next().await? {
            self.insert_root(key).await?;
        }

        Ok(())
    }

    /// Downgrade this [`BTreeWriteGuard`] into a [`BTreeReadGuard`].
    pub fn downgrade(self) -> BTreeReadGuard<S, C, FE> {
        BTreeReadGuard {
            schema: self.schema,
            collator: self.collator,
            dir: self.dir.downgrade(),
        }
    }
}

#[inline]
fn div_ceil(num: usize, denom: usize) -> usize {
    match num % denom {
        0 => num / denom,
        _ => (num / denom) + 1,
    }
}
