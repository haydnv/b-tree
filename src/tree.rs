use std::pin::Pin;
use std::sync::Arc;
use std::{fmt, io};

#[cfg(feature = "stream")]
use async_trait::async_trait;
use collate::{Collate, OverlapsValue};
#[cfg(feature = "stream")]
use destream::de;
use freqfs::*;
use futures::future::{self, Future, FutureExt};
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use futures::try_join;
use futures::TryFutureExt;
use safecast::AsType;
use smallvec::SmallVec;
use uuid::Uuid;

use super::node::Block;
use super::range::Range;
use super::{Collator, Key, Schema};

const NODE_STACK_SIZE: usize = 32;
type NodeStack<V> = SmallVec<[Key<V>; NODE_STACK_SIZE]>;

/// A read guard acquired on a [`BTreeLock`]
pub type BTreeReadGuard<S, C, FE> = BTree<S, C, Arc<DirReadGuardOwned<FE>>>;

/// A write guard acquired on a [`BTreeLock`]
pub type BTreeWriteGuard<S, C, FE> = BTree<S, C, DirWriteGuardOwned<FE>>;

// TODO: genericize
type Node<V> = super::node::Node<Vec<Vec<V>>>;

const ROOT: Uuid = Uuid::from_fields(0, 0, 0, &[0u8; 8]);

/// A futures-aware read-write lock on a [`BTree`]
pub struct BTreeLock<S, C, FE> {
    schema: Arc<S>,
    collator: Arc<Collator<C>>,
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
    /// Borrow the [`Collator`] used by this B+Tree.
    pub fn collator(&self) -> &Arc<Collator<C>> {
        &self.collator
    }

    /// Borrow the schema of the source B+Tree.
    pub fn schema(&self) -> &S {
        &self.schema
    }
}

impl<S, C, FE> BTreeLock<S, C, FE>
where
    S: Schema,
    FE: AsType<Node<S::Value>> + Send + Sync,
    Node<S::Value>: FileLoad,
{
    fn new(schema: S, collator: C, dir: DirLock<FE>) -> Self {
        Self {
            schema: Arc::new(schema),
            collator: Arc::new(Collator::new(collator)),
            dir,
        }
    }

    /// Create a new [`BTreeLock`] in `dir` with the given `collator`.
    pub fn create(schema: S, collator: C, dir: DirLock<FE>) -> Result<Self, io::Error> {
        let mut nodes = dir.try_write_owned()?;

        if nodes.is_empty() {
            nodes.create_file::<Node<S::Value>>(ROOT.to_string(), Node::Leaf(vec![]), 0)?;

            Ok(Self::new(schema, collator, dir))
        } else {
            Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "creating a new B+Tree requires an empty file",
            ))
        }
    }

    /// Load a [`BTreeLock`] with the given `schema` and `collator` from `dir`.
    pub fn load(schema: S, collator: C, dir: DirLock<FE>) -> Result<Self, io::Error> {
        let mut nodes = dir.try_write_owned()?;

        if !nodes.contains(&ROOT) {
            nodes.create_file(ROOT.to_string(), Node::Leaf(vec![]), 0)?;
        }

        Ok(Self::new(schema, collator, dir))
    }

    /// Write any modified blocks of thie [`BTree`] to the filesystem.
    pub async fn sync(&self) -> Result<(), io::Error>
    where
        FE: for<'a> freqfs::FileSave<'a>,
    {
        self.dir.sync().await
    }
}

impl<S, C, FE> BTreeLock<S, C, FE>
where
    FE: Send + Sync,
{
    /// Lock this B+Tree for reading, without borrowing.
    pub async fn into_read(self) -> BTreeReadGuard<S, C, FE> {
        #[cfg(feature = "logging")]
        log::debug!("lock B+Tree at {:?} for reading...", self.dir);

        self.dir
            .into_read()
            .map(Arc::new)
            .map(|dir| BTree {
                schema: self.schema.clone(),
                collator: self.collator.clone(),
                dir,
            })
            .await
    }

    /// Lock this B+Tree for reading.
    pub async fn read(&self) -> BTreeReadGuard<S, C, FE> {
        #[cfg(feature = "logging")]
        log::debug!("lock B+Tree at {:?} for reading...", self.dir);

        self.dir
            .read_owned()
            .map(Arc::new)
            .map(|dir| BTree {
                schema: self.schema.clone(),
                collator: self.collator.clone(),
                dir,
            })
            .await
    }

    /// Lock this B+Tree for reading synchronously, if possible.
    pub fn try_read(&self) -> Result<BTreeReadGuard<S, C, FE>, io::Error> {
        self.dir.try_read_owned().map(Arc::new).map(|dir| BTree {
            schema: self.schema.clone(),
            collator: self.collator.clone(),
            dir,
        })
    }

    /// Lock this B+Tree for writing, without borrowing.
    pub async fn into_write(self) -> BTreeWriteGuard<S, C, FE> {
        #[cfg(feature = "logging")]
        log::debug!("lock B+Tree at {:?} for writing...", self.dir);

        self.dir
            .into_write()
            .map(|dir| BTree {
                schema: self.schema.clone(),
                collator: self.collator.clone(),
                dir,
            })
            .await
    }

    /// Lock this B+Tree for writing.
    pub async fn write(&self) -> BTreeWriteGuard<S, C, FE> {
        #[cfg(feature = "logging")]
        log::debug!("lock B+Tree at {:?} for writing...", self.dir);

        self.dir
            .write_owned()
            .map(|dir| BTree {
                schema: self.schema.clone(),
                collator: self.collator.clone(),
                dir,
            })
            .await
    }

    /// Lock this B+Tree for writing synchronously, if possible.
    pub fn try_write(&self) -> Result<BTreeWriteGuard<S, C, FE>, io::Error> {
        self.dir.try_write_owned().map(|dir| BTree {
            schema: self.schema.clone(),
            collator: self.collator.clone(),
            dir,
        })
    }
}

#[cfg(feature = "stream")]
struct BTreeVisitor<S, C, FE> {
    btree: BTreeLock<S, C, FE>,
}

#[cfg(feature = "stream")]
#[async_trait]
impl<S, C, FE> de::Visitor for BTreeVisitor<S, C, FE>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Send + Sync,
    FE: AsType<Node<S::Value>> + Send + Sync,
    S::Value: de::FromStream<Context = ()>,
    Node<S::Value>: FileLoad,
{
    type Value = BTreeLock<S, C, FE>;

    fn expecting() -> &'static str {
        "a BTree"
    }

    async fn visit_seq<A: de::SeqAccess>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut btree = self.btree.write().await;

        while let Some(key) = seq.next_element(()).map_err(de::Error::custom).await? {
            btree.insert(key).map_err(de::Error::custom).await?;
        }

        Ok(self.btree)
    }
}

#[cfg(feature = "stream")]
#[async_trait]
impl<S, C, FE> de::FromStream for BTreeLock<S, C, FE>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Send + Sync,
    FE: AsType<Node<S::Value>> + Send + Sync,
    S::Value: de::FromStream<Context = ()>,
    Node<S::Value>: FileLoad,
{
    type Context = (S, C, DirLock<FE>);

    async fn from_stream<D: de::Decoder>(
        context: Self::Context,
        decoder: &mut D,
    ) -> Result<Self, D::Error> {
        let (schema, collator, dir) = context;
        let btree = BTreeLock::create(schema, collator, dir).map_err(de::Error::custom)?;
        decoder.decode_seq(BTreeVisitor { btree }).await
    }
}

type IntoStream<V> = Pin<Box<dyn Stream<Item = Result<Key<V>, io::Error>> + Send>>;

/// A B+Tree
pub struct BTree<S, C, G> {
    schema: Arc<S>,
    collator: Arc<Collator<C>>,
    dir: G,
}

impl<S, C, G> Clone for BTree<S, C, G>
where
    G: Clone,
{
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            collator: self.collator.clone(),
            dir: self.dir.clone(),
        }
    }
}

impl<S, C, G> BTree<S, C, G>
where
    S: Schema,
    C: Collate<Value = S::Value>,
{
    /// Borrow the [`Schema`] of this [`BTree`].
    pub fn schema(&self) -> &S {
        &self.schema
    }
}

impl<S, C, FE, G> BTree<S, C, G>
where
    S: Schema,
    C: Collate<Value = S::Value>,
    FE: AsType<Node<S::Value>> + Send + Sync,
    G: DirDeref<Entry = FE>,
    Node<S::Value>: FileLoad + fmt::Debug,
{
    /// Return `true` if this B+Tree contains the given `key`.
    pub async fn contains(&self, key: &[S::Value]) -> Result<bool, io::Error> {
        let mut node = self.dir.as_dir().read_file(&ROOT).await?;

        loop {
            match &*node {
                Node::Leaf(keys) => {
                    let i = keys.bisect_left(&key, &*self.collator);

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
                    let i = bounds.bisect_right(key, &*self.collator);

                    if i == 0 {
                        return Ok(false);
                    } else {
                        node = self.dir.as_dir().read_file(&children[i - 1]).await?;
                    }
                }
            }
        }
    }

    /// Count how many keys lie within the given `range` of this B+Tree.
    pub async fn count(&self, range: &Range<S::Value>) -> Result<u64, io::Error> {
        let root = self.dir.as_dir().read_file(&ROOT).await?;
        self.count_inner(range, root).await
    }

    fn count_inner<'a>(
        &'a self,
        range: &'a Range<S::Value>,
        node: FileReadGuard<'a, Node<S::Value>>,
    ) -> Pin<Box<dyn Future<Output = Result<u64, io::Error>> + 'a>> {
        Box::pin(async move {
            match &*node {
                Node::Leaf(keys) if range.is_default() => Ok(keys.len() as u64),
                Node::Leaf(keys) => {
                    let (l, r) = keys.bisect(range, &*self.collator);

                    if l == keys.len() {
                        Ok(0)
                    } else if l == r {
                        if range.contains_value(&keys[l], &*self.collator) {
                            Ok(1)
                        } else {
                            Ok(0)
                        }
                    } else {
                        Ok((r - l) as u64)
                    }
                }
                Node::Index(_bounds, children) if range.is_default() => {
                    stream::iter(children)
                        .then(|node_id| self.dir.as_dir().read_file(node_id))
                        .map_ok(|node| self.count_inner(range, node))
                        .try_buffer_unordered(num_cpus::get())
                        .try_fold(0, |sum, count| future::ready(Ok(sum + count)))
                        .await
                }
                Node::Index(bounds, children) => {
                    let (l, r) = bounds.bisect(range, &*self.collator);
                    let l = if l == 0 { l } else { l - 1 };

                    if l == children.len() {
                        let node = self
                            .dir
                            .as_dir()
                            .read_file(children.last().expect("last"))
                            .await?;

                        self.count_inner(range, node).await
                    } else if l == r || l + 1 == r {
                        let node = self.dir.as_dir().read_file(&children[l]).await?;
                        self.count_inner(range, node).await
                    } else {
                        let left = self
                            .dir
                            .as_dir()
                            .read_file(&children[l])
                            .and_then(|node| self.count_inner(range, node));

                        let default_range = Range::default();

                        let middle = stream::iter(&children[(l + 1)..(r - 1)])
                            .then(|node_id| self.dir.as_dir().read_file(node_id))
                            .map_ok(|node| self.count_inner(&default_range, node))
                            .try_buffer_unordered(num_cpus::get())
                            .try_fold(0, |sum, count| future::ready(Ok(sum + count)));

                        let right = self
                            .dir
                            .as_dir()
                            .read_file(&children[r - 1])
                            .and_then(|node| self.count_inner(range, node));

                        let (left, middle, right) = try_join!(left, middle, right)?;

                        Ok(left + middle + right)
                    }
                }
            }
        })
    }

    /// Return the first key in this B+Tree within the given `range`, if any.
    pub async fn first(&self, range: &Range<S::Value>) -> Result<Option<Key<S::Value>>, io::Error> {
        let mut node = self.dir.as_dir().read_file(&ROOT).await?;

        if let Node::Leaf(keys) = &*node {
            if keys.is_empty() {
                return Ok(None);
            }
        }

        Ok(loop {
            match &*node {
                Node::Leaf(keys) => {
                    let (l, _r) = keys.bisect(range, &self.collator);

                    break if l == keys.len() {
                        None
                    } else if range.contains_value(&keys[l], &self.collator) {
                        Some(stack_key(&keys[l]))
                    } else {
                        None
                    };
                }
                Node::Index(bounds, children) => {
                    let (l, _r) = bounds.bisect(range, &self.collator);

                    if l == bounds.len() {
                        node = self
                            .dir
                            .as_dir()
                            .read_file(children.last().expect("last"))
                            .await?;
                    } else if range.contains_value(&bounds[l], &self.collator) {
                        break Some(stack_key(&bounds[l]));
                    } else {
                        node = self.dir.as_dir().read_file(&children[l]).await?;
                    }
                }
            }
        })
    }

    /// Return the last key in this B+Tree with the given `prefix`, if any.
    pub async fn last(&self, range: &Range<S::Value>) -> Result<Option<Key<S::Value>>, io::Error> {
        let mut node = self.dir.as_dir().read_file(&ROOT).await?;

        if let Node::Leaf(keys) = &*node {
            if keys.is_empty() {
                return Ok(None);
            }
        }

        Ok(loop {
            match &*node {
                Node::Leaf(keys) => {
                    let (_l, r) = keys.bisect(range, &self.collator);

                    break if r == keys.len() {
                        if range.contains_value(&keys[r - 1], &self.collator) {
                            Some(stack_key(&keys[r - 1]))
                        } else {
                            None
                        }
                    } else if range.contains_value(&keys[r], &self.collator) {
                        Some(stack_key(&keys[r]))
                    } else {
                        None
                    };
                }
                Node::Index(bounds, children) => {
                    let (_l, r) = bounds.bisect(range, &self.collator);

                    if r == 0 {
                        break None;
                    } else {
                        node = self.dir.as_dir().read_file(&children[r - 1]).await?;
                    }
                }
            }
        })
    }

    /// Return `true` if the given `range` of this B+Tree contains zero keys.
    pub async fn is_empty(&self, range: &Range<S::Value>) -> Result<bool, io::Error> {
        let mut node = self.dir.as_dir().read_file(&ROOT).await?;

        Ok(loop {
            match &*node {
                Node::Leaf(keys) => {
                    let (l, r) = keys.bisect(range, &*self.collator);
                    break l == r;
                }
                Node::Index(bounds, children) => {
                    let (l, r) = bounds.bisect(range, &*self.collator);

                    if l == children.len() {
                        node = self.dir.as_dir().read_file(&children[l - 1]).await?;
                    } else if l == r {
                        node = self.dir.as_dir().read_file(&children[l]).await?;
                    } else {
                        break false;
                    }
                }
            }
        })
    }

    #[cfg(debug_assertions)]
    fn is_valid_node<'a>(
        &'a self,
        node: &'a Node<S::Value>,
    ) -> Pin<Box<dyn Future<Output = Result<bool, io::Error>> + 'a>> {
        Box::pin(async move {
            let order = self.schema.order();

            match &*node {
                Node::Leaf(keys) => {
                    assert!(keys.len() >= (order / 2) - 1);
                    assert!(keys.len() < order);
                }
                Node::Index(bounds, children) => {
                    assert_eq!(bounds.len(), children.len());
                    assert!(children.len() >= self.schema.order() / 2);
                    assert!(children.len() <= order);

                    for (left, node_id) in bounds.iter().zip(children) {
                        let node = self.dir.as_dir().read_file(node_id).await?;

                        match &*node {
                            Node::Leaf(keys) => assert_eq!(left, &keys[0]),
                            Node::Index(bounds, _) => assert_eq!(left, &bounds[0]),
                        }

                        assert!(self.is_valid_node(&*node).await?);
                    }
                }
            }

            Ok(true)
        })
    }
}

impl<S, C, FE, G> BTree<S, C, G>
where
    S: Schema,
    C: Collate<Value = S::Value> + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static,
    G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    Node<S::Value>: FileLoad + fmt::Debug,
{
    /// Construct a [`Stream`] of all the keys in the given `range` of this B+Tree.
    pub fn keys<R: Into<Arc<Range<S::Value>>>>(
        self,
        range: R,
        reverse: bool,
    ) -> impl Stream<Item = Result<Key<S::Value>, io::Error>> + Unpin + Send + Sized {
        let range = range.into();

        if reverse {
            keys_reverse(self.dir, self.collator, range, ROOT)
        } else {
            keys_forward(self.dir, self.collator, range, ROOT)
        }
    }

    #[cfg(debug_assertions)]
    pub async fn is_valid(self) -> Result<bool, io::Error> {
        {
            let root = self.dir.as_dir().read_file(&ROOT).await?;

            match &*root {
                Node::Leaf(keys) => {
                    assert!(keys.len() <= self.schema.order());
                }
                Node::Index(bounds, children) => {
                    assert_eq!(bounds.len(), children.len());

                    for (left, node_id) in bounds.iter().zip(children) {
                        let node = self.dir.as_dir().read_file(node_id).await?;

                        match &*node {
                            Node::Leaf(keys) => assert_eq!(left, &keys[0]),
                            Node::Index(bounds, _) => assert_eq!(left, &bounds[0]),
                        }

                        assert!(self.is_valid_node(&*node).await?);
                    }
                }
            }
        }

        let range = Range::default();
        let count = self.count(&range).await? as usize;
        let mut contents = Vec::with_capacity(count);
        let mut stream = self.keys(range, false);
        while let Some(key) = stream.try_next().await? {
            contents.push(key);
        }

        assert_eq!(count, contents.len());

        Ok(true)
    }
}

fn keys_forward<C, V, FE, G>(
    dir: G,
    collator: Arc<Collator<C>>,
    range: Arc<Range<V>>,
    node_id: Uuid,
) -> IntoStream<V>
where
    C: Collate<Value = V> + Send + Sync + 'static,
    V: Clone + PartialEq + fmt::Debug + Send + Sync + 'static,
    FE: AsType<Node<V>> + Send + Sync + 'static,
    G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    Node<V>: FileLoad,
{
    #[cfg(feature = "logging")]
    log::debug!("reading BTree keys in forward order");

    let file = dir.as_dir().get_file(&node_id).expect("node").clone();
    let fut = file.into_read().map_ok(move |node| {
        #[cfg(feature = "logging")]
        log::debug!("locked node for reading");

        let keys: IntoStream<V> = match &*node {
            Node::Leaf(keys) if range.is_default() => {
                let keys = keys.iter().map(stack_key).collect::<NodeStack<V>>();

                Box::pin(stream::iter(keys).map(Ok))
            }
            Node::Leaf(keys) => {
                let (l, r) = keys.bisect(&*range, &*collator);

                if l == keys.len() || r == 0 {
                    Box::pin(stream::empty())
                } else if l == r {
                    if range.contains_value(&keys[l], &*collator) {
                        Box::pin(stream::once(future::ready(Ok(stack_key(&keys[l])))))
                    } else {
                        Box::pin(stream::empty())
                    }
                } else {
                    let keys = keys[l..r].iter().map(stack_key).collect::<NodeStack<V>>();

                    Box::pin(stream::iter(keys).map(Ok))
                }
            }
            Node::Index(_bounds, children) if range.is_default() => {
                let children = SmallVec::<[Uuid; NODE_STACK_SIZE]>::from_slice(children);

                let keys = stream::iter(children)
                    .map(move |node_id| {
                        #[cfg(feature = "logging")]
                        log::debug!("reading keys from child node...");

                        keys_forward(dir.clone(), collator.clone(), range.clone(), node_id)
                    })
                    .flatten();

                Box::pin(keys)
            }
            Node::Index(bounds, children) => {
                let (l, r) = bounds.bisect(&*range, &*collator);
                let l = if l == 0 { l } else { l - 1 };

                if r == 0 {
                    let empty: IntoStream<V> = Box::pin(stream::empty());
                    return empty;
                } else if l == children.len() {
                    #[cfg(feature = "logging")]
                    log::debug!("reading keys from child node {}...", l - 1);

                    keys_forward(dir, collator, range, children[l - 1])
                } else if l == r || l + 1 == r {
                    #[cfg(feature = "logging")]
                    log::debug!("reading keys from child node {}...", l);

                    keys_forward(dir, collator, range, children[l])
                } else {
                    #[cfg(feature = "logging")]
                    log::debug!("reading keys from child nodes {}..{}", l, r);

                    let left =
                        keys_forward(dir.clone(), collator.clone(), range.clone(), children[l]);

                    let right = keys_forward(dir.clone(), collator.clone(), range, children[r - 1]);

                    let default_range = Arc::new(Range::default());

                    let children = SmallVec::<[Uuid; NODE_STACK_SIZE]>::from_slice(
                        &children[(l + 1)..(r - 1)],
                    );

                    let middle = stream::iter(children)
                        .map(move |node_id| {
                            keys_forward(
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

fn keys_reverse<C, V, FE, G>(
    dir: G,
    collator: Arc<Collator<C>>,
    range: Arc<Range<V>>,
    node_id: Uuid,
) -> IntoStream<V>
where
    C: Collate<Value = V> + Send + Sync + 'static,
    V: Clone + PartialEq + fmt::Debug + Send + Sync + 'static,
    FE: AsType<Node<V>> + Send + Sync + 'static,
    G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    Node<V>: FileLoad,
{
    let file = dir.as_dir().get_file(&node_id).expect("node").clone();
    let fut = file.into_read().map_ok(move |node| {
        let keys: IntoStream<V> = match &*node {
            Node::Leaf(keys) if range.is_default() => {
                let keys = keys.iter().rev().map(stack_key).collect::<NodeStack<V>>();

                Box::pin(stream::iter(keys.into_iter().map(Ok)))
            }
            Node::Leaf(keys) => {
                let (l, r) = keys.bisect(&*range, &*collator);

                if l == keys.len() || r == 0 {
                    Box::pin(stream::empty())
                } else if l == r {
                    if range.contains_value(&keys[l], &*collator) {
                        Box::pin(stream::once(future::ready(Ok(stack_key(&keys[l])))))
                    } else {
                        Box::pin(stream::empty())
                    }
                } else {
                    let keys = keys[l..r]
                        .iter()
                        .rev()
                        .map(stack_key)
                        .collect::<NodeStack<V>>();

                    Box::pin(stream::iter(keys.into_iter().map(Ok)))
                }
            }
            Node::Index(_bounds, children) if range.is_default() => {
                let children = children
                    .iter()
                    .rev()
                    .copied()
                    .collect::<SmallVec<[Uuid; NODE_STACK_SIZE]>>();

                let keys = stream::iter(children)
                    .map(move |node_id| {
                        keys_reverse(dir.clone(), collator.clone(), range.clone(), node_id)
                    })
                    .flatten();

                Box::pin(keys)
            }
            Node::Index(bounds, children) => {
                let (l, r) = bounds.bisect(&*range, &*collator);
                let l = if l == 0 { l } else { l - 1 };

                if r == 0 {
                    let empty: IntoStream<V> = Box::pin(stream::empty());
                    return empty;
                } else if l == children.len() {
                    keys_reverse(dir, collator, range, children[l - 1])
                } else if l == r || l + 1 == r {
                    keys_reverse(dir, collator, range, children[l])
                } else {
                    let left =
                        keys_reverse(dir.clone(), collator.clone(), range.clone(), children[l]);

                    let right = keys_reverse(dir.clone(), collator.clone(), range, children[r - 1]);

                    let default_range = Arc::new(Range::default());

                    let middle_children = children[(l + 1)..(r - 1)]
                        .iter()
                        .rev()
                        .copied()
                        .collect::<SmallVec<[Uuid; NODE_STACK_SIZE]>>();

                    let middle = stream::iter(middle_children)
                        .map(move |node_id| {
                            keys_reverse(
                                dir.clone(),
                                collator.clone(),
                                default_range.clone(),
                                node_id,
                            )
                        })
                        .flatten();

                    Box::pin(right.chain(middle).chain(left))
                }
            }
        };

        keys
    });

    Box::pin(stream::once(fut).try_flatten())
}

enum MergeIndexLeft<V> {
    Borrow(Vec<V>),
    Merge(Vec<V>),
}

enum MergeIndexRight {
    Borrow,
    Merge,
}

enum MergeLeafLeft<V> {
    Borrow(Vec<V>),
    Merge(Vec<V>),
}

enum MergeLeafRight {
    Borrow,
    Merge,
}

enum Delete<FE, V> {
    None,
    Left(Vec<V>),
    Right,
    Underflow(FileWriteGuardOwned<FE, Node<V>>),
}

enum Insert<V> {
    None,
    Left(Vec<V>),
    Right,
    OverflowLeft(Vec<V>, Vec<V>, Uuid),
    Overflow(Vec<V>, Uuid),
}

impl<S, C, FE> BTree<S, C, DirWriteGuardOwned<FE>> {
    /// Downgrade this [`BTreeWriteGuard`] into a [`BTreeReadGuard`].
    pub fn downgrade(self) -> BTreeReadGuard<S, C, FE> {
        BTreeReadGuard {
            schema: self.schema,
            collator: self.collator,
            dir: Arc::new(self.dir.downgrade()),
        }
    }
}

impl<S, C, FE> BTree<S, C, DirWriteGuardOwned<FE>>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Send + Sync,
    FE: AsType<Node<S::Value>> + Send + Sync,
    Node<S::Value>: FileLoad,
{
    /// Delete the given `key` from this B+Tree.
    pub async fn delete(&mut self, key: &[S::Value]) -> Result<bool, io::Error> {
        let mut root = self.dir.write_file_owned(&ROOT).await?;

        let new_root = match &mut *root {
            Node::Leaf(keys) => {
                let i = keys.bisect_left(&key, &*self.collator);
                if i < keys.len() && &keys[i] == key {
                    keys.remove(i);
                    return Ok(true);
                } else {
                    return Ok(false);
                }
            }
            Node::Index(bounds, children) => {
                let i = match bounds.bisect_right(&key, &*self.collator) {
                    0 => return Ok(false),
                    i => i - 1,
                };

                let node = self.dir.write_file_owned(&children[i]).await?;
                match self.delete_inner(node, &key).await? {
                    Delete::None => return Ok(false),
                    Delete::Right => return Ok(true),
                    Delete::Left(bound) => {
                        bounds[i] = bound;
                        return Ok(true);
                    }
                    Delete::Underflow(mut node) => match &mut *node {
                        Node::Leaf(new_keys) => {
                            self.merge_leaf(new_keys, i, bounds, children).await?
                        }
                        Node::Index(new_bounds, new_children) => {
                            self.merge_index(new_bounds, new_children, i, bounds, children)
                                .await?
                        }
                    },
                }

                if children.len() == 1 {
                    bounds.pop();
                    children.pop()
                } else {
                    None
                }
            }
        };

        if let Some(only_child) = new_root {
            let new_root = {
                let mut child = self.dir.write_file(&only_child).await?;
                match &mut *child {
                    Node::Leaf(keys) => Node::Leaf(keys.drain(..).collect()),
                    Node::Index(bounds, children) => {
                        Node::Index(bounds.drain(..).collect(), children.drain(..).collect())
                    }
                }
            };

            self.dir.delete(&only_child).await;

            *root = new_root;
        }

        Ok(true)
    }

    fn delete_inner<'a>(
        &'a mut self,
        mut node: FileWriteGuardOwned<FE, Node<S::Value>>,
        key: &'a [S::Value],
    ) -> Pin<Box<dyn Future<Output = Result<Delete<FE, S::Value>, io::Error>> + Send + 'a>> {
        Box::pin(async move {
            match &mut *node {
                Node::Leaf(keys) => {
                    let i = keys.bisect_left(&key, &*self.collator);

                    if i < keys.len() && &keys[i] == key {
                        keys.remove(i);

                        if keys.len() < (self.schema.order() / 2) {
                            Ok(Delete::Underflow(node))
                        } else if i == 0 {
                            Ok(Delete::Left(keys[0].to_vec()))
                        } else {
                            Ok(Delete::Right)
                        }
                    } else {
                        Ok(Delete::None)
                    }
                }
                Node::Index(bounds, children) => {
                    let i = match bounds.bisect_right(key, &*self.collator) {
                        0 => return Ok(Delete::None),
                        i => i - 1,
                    };

                    let child = self.dir.write_file_owned(&children[i]).await?;
                    match self.delete_inner(child, key).await? {
                        Delete::None => return Ok(Delete::None),
                        Delete::Right => return Ok(Delete::Right),
                        Delete::Left(bound) => {
                            bounds[i] = bound;

                            return if i == 0 {
                                Ok(Delete::Left(bounds[0].to_vec()))
                            } else {
                                Ok(Delete::Right)
                            };
                        }
                        Delete::Underflow(mut node) => match &mut *node {
                            Node::Leaf(new_keys) => {
                                self.merge_leaf(new_keys, i, bounds, children).await?
                            }
                            Node::Index(new_bounds, new_children) => {
                                self.merge_index(new_bounds, new_children, i, bounds, children)
                                    .await?
                            }
                        },
                    }

                    if children.len() > (self.schema.order() / 2) {
                        if i == 0 {
                            Ok(Delete::Left(bounds[0].to_vec()))
                        } else {
                            Ok(Delete::Right)
                        }
                    } else {
                        Ok(Delete::Underflow(node))
                    }
                }
            }
        })
    }

    fn merge_index<'a>(
        &'a mut self,
        new_bounds: &'a mut Vec<Vec<S::Value>>,
        new_children: &'a mut Vec<Uuid>,
        i: usize,
        bounds: &'a mut Vec<Vec<S::Value>>,
        children: &'a mut Vec<Uuid>,
    ) -> Pin<Box<dyn Future<Output = Result<(), io::Error>> + Send + 'a>> {
        Box::pin(async move {
            if i == 0 {
                match self
                    .merge_index_left(new_bounds, new_children, &children[i + 1])
                    .await?
                {
                    MergeIndexLeft::Borrow(bound) => {
                        bounds[i] = new_bounds[0].to_vec();
                        bounds[i + 1] = bound;
                    }
                    MergeIndexLeft::Merge(bound) => {
                        self.dir.delete(&children[0]).await;
                        children.remove(0);
                        bounds.remove(0);
                        bounds[0] = bound;
                    }
                }
            } else {
                match self
                    .merge_index_right(new_bounds, new_children, &children[i - 1])
                    .await?
                {
                    MergeIndexRight::Borrow => {
                        bounds[i] = new_bounds[0].to_vec();
                    }
                    MergeIndexRight::Merge => {
                        self.dir.delete(&children[i]).await;
                        children.remove(i);
                        bounds.remove(i);
                    }
                }
            }

            Ok(())
        })
    }

    fn merge_index_left<'a>(
        &'a self,
        left_bounds: &'a mut Vec<Vec<S::Value>>,
        left_children: &'a mut Vec<Uuid>,
        node_id: &'a Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<MergeIndexLeft<S::Value>, io::Error>> + Send + 'a>>
    {
        Box::pin(async move {
            let mut node = self.dir.write_file(node_id).await?;

            match &mut *node {
                Node::Leaf(_right_keys) => unreachable!("merge a leaf node with an index node"),
                Node::Index(right_bounds, right_children) => {
                    if right_bounds.len() > (self.schema.order() / 2) {
                        left_bounds.push(right_bounds.remove(0));
                        left_children.push(right_children.remove(0));
                        Ok(MergeIndexLeft::Borrow(right_bounds[0].to_vec()))
                    } else {
                        let mut new_bounds =
                            Vec::with_capacity(left_bounds.len() + right_bounds.len());

                        new_bounds.extend(left_bounds.drain(..));
                        new_bounds.extend(right_bounds.drain(..));
                        *right_bounds = new_bounds;

                        let mut new_children = Vec::with_capacity(right_bounds.len());

                        new_children.extend(left_children.drain(..));
                        new_children.extend(right_children.drain(..));
                        *right_children = new_children;

                        Ok(MergeIndexLeft::Merge(right_bounds[0].to_vec()))
                    }
                }
            }
        })
    }

    fn merge_index_right<'a>(
        &'a self,
        right_bounds: &'a mut Vec<Vec<S::Value>>,
        right_children: &'a mut Vec<Uuid>,
        node_id: &'a Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<MergeIndexRight, io::Error>> + Send + 'a>> {
        Box::pin(async move {
            let mut node = self.dir.write_file(node_id).await?;

            match &mut *node {
                Node::Leaf(_left_keys) => unreachable!("merge a leaf node with an index node"),
                Node::Index(left_bounds, left_children) => {
                    if left_children.len() > (self.schema.order() / 2) {
                        let right = left_bounds.pop().expect("right");
                        right_bounds.insert(0, right);

                        let right = left_children.pop().expect("right");
                        right_children.insert(0, right);

                        Ok(MergeIndexRight::Borrow)
                    } else {
                        left_bounds.extend(right_bounds.drain(..));
                        left_children.extend(right_children.drain(..));
                        Ok(MergeIndexRight::Merge)
                    }
                }
            }
        })
    }

    fn merge_leaf<'a>(
        &'a mut self,
        new_keys: &'a mut Vec<Vec<S::Value>>,
        i: usize,
        bounds: &'a mut Vec<Vec<S::Value>>,
        children: &'a mut Vec<Uuid>,
    ) -> Pin<Box<dyn Future<Output = Result<(), io::Error>> + Send + 'a>> {
        Box::pin(async move {
            if i == 0 {
                match self.merge_leaf_left(new_keys, &children[i + 1]).await? {
                    MergeLeafLeft::Borrow(bound) => {
                        bounds[i] = new_keys[0].to_vec();
                        bounds[i + 1] = bound;
                    }
                    MergeLeafLeft::Merge(bound) => {
                        self.dir.delete(&children[0]).await;
                        children.remove(0);
                        bounds.remove(0);
                        bounds[0] = bound;
                    }
                }
            } else {
                match self.merge_leaf_right(new_keys, &children[i - 1]).await? {
                    MergeLeafRight::Borrow => {
                        bounds[i] = new_keys[0].to_vec();
                    }
                    MergeLeafRight::Merge => {
                        self.dir.delete(&children[i]).await;
                        children.remove(i);
                        bounds.remove(i);
                    }
                }
            }

            Ok(())
        })
    }

    fn merge_leaf_left<'a>(
        &'a self,
        left_keys: &'a mut Vec<Vec<S::Value>>,
        node_id: &'a Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<MergeLeafLeft<S::Value>, io::Error>> + Send + 'a>> {
        Box::pin(async move {
            let mut node = self.dir.write_file(node_id).await?;

            match &mut *node {
                Node::Leaf(right_keys) => {
                    if right_keys.len() > (self.schema.order() / 2) {
                        left_keys.push(right_keys.remove(0));
                        Ok(MergeLeafLeft::Borrow(right_keys[0].to_vec()))
                    } else {
                        let mut new_keys = Vec::with_capacity(left_keys.len() + right_keys.len());
                        new_keys.extend(left_keys.drain(..));
                        new_keys.extend(right_keys.drain(..));
                        *right_keys = new_keys;

                        Ok(MergeLeafLeft::Merge(right_keys[0].to_vec()))
                    }
                }
                Node::Index(bounds, children) => {
                    match self.merge_leaf_left(left_keys, &children[0]).await? {
                        MergeLeafLeft::Borrow(left) => {
                            bounds[0] = left.to_vec();
                            Ok(MergeLeafLeft::Borrow(left))
                        }
                        MergeLeafLeft::Merge(left) => {
                            bounds[0] = left.to_vec();
                            Ok(MergeLeafLeft::Merge(left))
                        }
                    }
                }
            }
        })
    }

    fn merge_leaf_right<'a>(
        &'a self,
        right_keys: &'a mut Vec<Vec<S::Value>>,
        node_id: &'a Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<MergeLeafRight, io::Error>> + Send + 'a>> {
        Box::pin(async move {
            let mut node = self.dir.write_file(node_id).await?;

            match &mut *node {
                Node::Leaf(left_keys) => {
                    if left_keys.len() > (self.schema.order() / 2) {
                        let right = left_keys.pop().expect("right");
                        right_keys.insert(0, right);
                        Ok(MergeLeafRight::Borrow)
                    } else {
                        left_keys.extend(right_keys.drain(..));
                        Ok(MergeLeafRight::Merge)
                    }
                }
                Node::Index(_bounds, _children) => unreachable!("merge with the rightmost leaf"),
            }
        })
    }

    /// Insert the given `key` into this B+Tree.
    /// Return `false` if te given `key` is already present.
    pub async fn insert(&mut self, key: Vec<S::Value>) -> Result<bool, io::Error> {
        let key = validate_key(&*self.schema, key)?;
        self.insert_root(key).await
    }

    async fn insert_root(&mut self, key: Vec<S::Value>) -> Result<bool, io::Error> {
        let order = self.schema.order();
        let mut root = self.dir.write_file_owned(&ROOT).await?;

        let new_root = match &mut *root {
            Node::Leaf(keys) => {
                let i = keys.bisect_left(&key, &*self.collator);

                if keys.get(i) == Some(&key) {
                    // no-op
                    return Ok(false);
                }

                keys.insert(i, key);

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

                let i = match bounds.bisect_left(&key, &*self.collator) {
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

                // debug_assert!(self.collator.is_sorted(&bounds));
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
        key: Vec<S::Value>,
    ) -> Pin<Box<dyn Future<Output = Result<Insert<S::Value>, io::Error>> + Send + 'a>> {
        Box::pin(async move {
            let order = self.schema.order();

            match node {
                Node::Leaf(keys) => {
                    let i = keys.bisect_left(&key, &*self.collator);

                    if i < keys.len() && keys[i] == key {
                        // no-op
                        return Ok(Insert::None);
                    }

                    keys.insert(i, key);

                    let mid = order / 2;

                    if keys.len() >= order {
                        let size = self.schema.block_size() / 2;
                        let new_leaf: Vec<_> = keys.drain(mid..).collect();

                        debug_assert!(new_leaf.len() >= mid);
                        debug_assert!(keys.len() >= mid);

                        let middle_key = new_leaf[0].to_vec();
                        let node = Node::Leaf(new_leaf);
                        let (new_node_id, _) = self.dir.create_file_unique(node, size)?;

                        if i == 0 {
                            Ok(Insert::OverflowLeft(
                                keys[0].to_vec(),
                                middle_key,
                                new_node_id,
                            ))
                        } else {
                            Ok(Insert::Overflow(middle_key, new_node_id))
                        }
                    } else {
                        debug_assert!(keys.len() > mid);

                        if i == 0 {
                            Ok(Insert::Left(keys[0].to_vec()))
                        } else {
                            Ok(Insert::Right)
                        }
                    }
                }
                Node::Index(bounds, children) => {
                    debug_assert_eq!(bounds.len(), children.len());
                    let size = self.schema.block_size() >> 1;

                    let i = match bounds.bisect_left(&key, &*self.collator) {
                        0 => 0,
                        i => i - 1,
                    };

                    let mut child = self.dir.write_file_owned(&children[i]).await?;

                    let overflow_left = match self.insert_inner(&mut child, key).await? {
                        Insert::None => return Ok(Insert::None),
                        Insert::Right => return Ok(Insert::Right),
                        Insert::Left(key) => {
                            bounds[i] = key;

                            return if i == 0 {
                                Ok(Insert::Left(bounds[i].to_vec()))
                            } else {
                                Ok(Insert::Right)
                            };
                        }
                        Insert::OverflowLeft(left, middle, child_id) => {
                            bounds[i] = left;
                            bounds.insert(i + 1, middle);
                            children.insert(i + 1, child_id);
                            i == 0
                        }
                        Insert::Overflow(bound, child_id) => {
                            bounds.insert(i + 1, bound);
                            children.insert(i + 1, child_id);
                            false
                        }
                    };

                    // debug_assert!(self.collator.is_sorted(bounds));

                    if children.len() > order {
                        let mid = div_ceil(self.schema.order(), 2);
                        let new_bounds: Vec<_> = bounds.drain(mid..).collect();
                        let new_children: Vec<_> = children.drain(mid..).collect();

                        let left_bound = new_bounds[0].to_vec();
                        let node = Node::Index(new_bounds, new_children);
                        let (node_id, _) = self.dir.create_file_unique(node, size)?;

                        if overflow_left {
                            Ok(Insert::OverflowLeft(
                                bounds[0].to_vec(),
                                left_bound,
                                node_id,
                            ))
                        } else {
                            Ok(Insert::Overflow(left_bound, node_id))
                        }
                    } else if i == 0 {
                        Ok(Insert::Left(bounds[0].to_vec()))
                    } else {
                        Ok(Insert::Right)
                    }
                }
            }
        })
    }

    /// Delete all the keys in this [`BTree`].
    pub async fn truncate(&mut self) -> Result<(), io::Error> {
        self.dir.truncate().await;

        self.dir
            .create_file(ROOT.to_string(), Node::Leaf(vec![]), 0)?;

        Ok(())
    }
}

impl<S, C, FE> BTree<S, C, DirWriteGuardOwned<FE>>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static,
    Node<S::Value>: FileLoad,
{
    /// Merge the keys from the `other` B+Tree range into this one.
    ///
    /// The source B+Tree **must** have an identical schema and collation.
    pub async fn merge<G>(&mut self, other: BTree<S, C, G>) -> Result<(), io::Error>
    where
        G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    {
        validate_collator_eq(&self.collator, &other.collator)?;
        validate_schema_eq(&self.schema, &other.schema)?;

        let mut keys = other.keys(Range::default(), false);
        while let Some(key) = keys.try_next().await? {
            self.insert_root(key.into_vec()).await?;
        }

        Ok(())
    }

    /// Delete the keys in the `other` B+Tree from this one.
    ///
    /// The source B+Tree **must** have an identical schema and collation.
    pub async fn delete_all<G>(&mut self, other: BTree<S, C, G>) -> Result<(), io::Error>
    where
        G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    {
        validate_collator_eq(&self.collator, &other.collator)?;
        validate_schema_eq(&self.schema, &other.schema)?;

        let mut keys = other.keys(Range::default(), false);
        while let Some(key) = keys.try_next().await? {
            self.delete(&key).await?;
        }

        Ok(())
    }
}

#[inline]
fn validate_key<S: Schema>(schema: &S, key: Vec<S::Value>) -> Result<Vec<S::Value>, io::Error> {
    schema
        .validate_key(key)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))
}

#[inline]
fn validate_collator_eq<S>(this: &S, that: &S) -> Result<(), io::Error>
where
    S: PartialEq,
{
    if this == that {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "B+Tree to merge must have the same collation",
        )
        .into())
    }
}

#[inline]
fn validate_schema_eq<S>(this: &S, that: &S) -> Result<(), io::Error>
where
    S: PartialEq + fmt::Debug,
{
    if this == that {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "cannot merge a B+Tree with schema {:?} into one with schema {:?}",
                that, this
            ),
        )
        .into())
    }
}

#[inline]
fn div_ceil(num: usize, denom: usize) -> usize {
    match num % denom {
        0 => num / denom,
        _ => (num / denom) + 1,
    }
}

#[inline]
fn stack_key<'a, T, A>(iter: A) -> SmallVec<[T; NODE_STACK_SIZE]>
where
    T: Clone + 'a,
    A: IntoIterator<Item = &'a T>,
{
    iter.into_iter().cloned().collect()
}
