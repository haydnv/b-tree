use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::{fmt, io};

use async_trait::async_trait;
use bytes::Bytes;
use collate::Collator;
use destream::{de, en};
use freqfs::{Cache, FileLoad};
use futures::{TryFutureExt, TryStreamExt};
use rand::Rng;
use safecast::as_type;
use tokio::fs;
use tokio_util::io::StreamReader;

use b_tree::{BTreeLock, BTreeReadGuard, Node, Range, Schema};

const BLOCK_SIZE: usize = 4_096;

enum File {
    Node(Node<i16>),
}

as_type!(File, Node, Node<i16>);

struct FileVisitor;

#[async_trait]
impl de::Visitor for FileVisitor {
    type Value = File;

    fn expecting() -> &'static str {
        "a B+Tree node"
    }

    async fn visit_seq<A: de::SeqAccess>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let leaf = seq.expect_next::<bool>(()).await?;

        if leaf {
            seq.expect_next(())
                .map_ok(Node::Leaf)
                .map_ok(File::Node)
                .await
        } else {
            seq.expect_next(())
                .map_ok(|(bounds, children)| Node::Index(bounds, children))
                .map_ok(File::Node)
                .await
        }
    }
}

#[async_trait]
impl de::FromStream for File {
    type Context = ();

    async fn from_stream<D: de::Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
        decoder.decode_seq(FileVisitor).await
    }
}

impl<'en> en::ToStream<'en> for File {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        use en::IntoStream;

        match self {
            Self::Node(node) => match node {
                Node::Leaf(keys) => (true, keys).into_stream(encoder),
                Node::Index(bounds, children) => (false, (bounds, children)).into_stream(encoder),
            },
        }
    }
}

#[async_trait]
impl FileLoad for File {
    async fn load(
        _path: &Path,
        file: fs::File,
        _metadata: std::fs::Metadata,
    ) -> Result<Self, io::Error> {
        destream_json::de::read_from((), file)
            .map_err(|cause| io::Error::new(io::ErrorKind::InvalidData, cause))
            .await
    }

    async fn save(&self, file: &mut fs::File) -> Result<u64, io::Error> {
        let encoded = destream_json::en::encode(self)
            .map_err(|cause| io::Error::new(io::ErrorKind::InvalidData, cause))?;

        let mut reader = StreamReader::new(
            encoded
                .map_ok(Bytes::from)
                .map_err(|cause| io::Error::new(io::ErrorKind::InvalidData, cause)),
        );

        let size = tokio::io::copy(&mut reader, file).await?;
        assert!(size > 0);
        Ok(size)
    }
}

#[derive(Debug)]
struct ExampleSchema<T> {
    size: usize,
    value: PhantomData<T>,
}

impl<T> ExampleSchema<T> {
    fn new(size: usize) -> Self {
        Self {
            size,
            value: PhantomData,
        }
    }
}

impl<T> PartialEq for ExampleSchema<T> {
    fn eq(&self, other: &Self) -> bool {
        self.size == other.size
    }
}

impl<T> Eq for ExampleSchema<T> {}

impl<T: fmt::Debug> Schema for ExampleSchema<T> {
    type Error = io::Error;
    type Value = i16;

    fn block_size(&self) -> usize {
        BLOCK_SIZE
    }

    fn order(&self) -> usize {
        5
    }

    fn validate(&self, key: Vec<i16>) -> Result<Vec<i16>, io::Error> {
        if key.len() == self.size {
            Ok(key)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("key length should be {}", self.size),
            ))
        }
    }
}

async fn setup_tmp_dir() -> Result<PathBuf, io::Error> {
    let mut rng = rand::thread_rng();
    loop {
        let rand: u32 = rng.gen();
        let path = PathBuf::from(format!("/tmp/test_btree_{}", rand));
        if !path.exists() {
            fs::create_dir(&path).await?;
            break Ok(path);
        }
    }
}

async fn functional_test() -> Result<(), io::Error> {
    // set up the test directory
    let path = setup_tmp_dir().await?;

    // construct the schema
    let schema = ExampleSchema::<i16>::new(3);

    // initialize the cache
    let cache = Cache::<File>::new(schema.block_size(), None);

    // load the directory and file paths into memory (not file contents, yet)
    let dir = cache.load(path.clone())?;

    // create a new B+ tree
    let btree = BTreeLock::create(schema, Collator::<i16>::default(), dir)?;

    {
        let mut view = btree.write().await;

        assert!(view.is_empty(&Range::default()).await?);
        assert_eq!(view.count(&Range::default()).await?, 0);

        let n = 300;

        for i in 1..n {
            let lo = i;
            let hi = i16::MAX - lo;
            let spread = hi - lo;

            let key = vec![lo, hi, spread];

            assert!(!view.contains(&key).await?);
            assert!(view.is_empty(&Range::with_prefix(vec![i])).await?);

            assert!(view.insert(key.clone()).await?);

            assert!(view.contains(&key).await?);
            assert!(!view.is_empty(&Range::with_prefix(vec![i])).await?);

            assert_eq!(view.count(&Range::new(vec![], 0..i)).await?, (i as u64) - 1);
            assert_eq!(view.count(&Range::with_prefix(vec![i])).await?, 1);
            assert_eq!(view.count(&Range::default()).await?, i as u64);

            #[cfg(debug_assertions)]
            assert!(view.is_valid().await?);
        }

        let mut i = 1;

        {
            let range = Range::new(vec![], 0..67);
            let mut nodes = view.to_stream(&range);
            while let Some(node) = nodes.try_next().await? {
                for key in &*node {
                    assert_eq!(key[0], i);
                    i += 1;
                }
            }
        }

        {
            let range = Range::new(vec![], 67..250);
            let mut nodes = view.to_stream(&range);
            while let Some(node) = nodes.try_next().await? {
                for key in &*node {
                    assert_eq!(key[0], i);
                    i += 1;
                }
            }
        }

        let mut i = 1;
        let mut keys = view.into_stream(Range::new(vec![], 0..123), false);
        while let Some(key) = keys.try_next().await? {
            assert_eq!(key[0], i);
            i += 1;
        }

        let view = btree.read().await;
        let mut keys = view.into_stream(Range::new(vec![], 123..n), false);
        while let Some(key) = keys.try_next().await? {
            assert_eq!(key[0], i);
            i += 1;
        }

        let view = btree.read().await;
        assert_eq!(view.count(&Range::default()).await?, (n - 1) as u64);

        for i in 1..n {
            let count = (i as u64) - 1;
            let range_left = Range::new(vec![], 0..i);

            assert_eq!(
                count_keys(&view, &range_left).await?,
                count,
                "bad key count at {}",
                i
            );

            assert_eq!(view.count(&range_left).await?, count, "bad count at {}", i);
        }

        std::mem::drop(view);

        let view = btree.write().await;

        for i in 1..n {
            let key = vec![i, i16::MAX - i, i16::MAX - 2 * i];
            assert!(view.contains(&key).await?);
        }

        let mut i = n - 1;
        let mut reversed = view.into_stream(Range::default(), true);
        while let Some(key) = reversed.try_next().await? {
            assert_eq!(key[0], i);
            i -= 1;
        }
        assert_eq!(i, 0);

        let mut view = btree.write().await;
        let mut count = view.count(&Range::default()).await?;
        assert_eq!(count, (n - 1) as u64);
        assert!(!view.is_empty(&Range::default()).await?);

        while !view.is_empty(&Range::default()).await? {
            let lo = view.first().await?.expect("first")[0];
            let hi = view.last().await?.expect("last")[0];

            let i = rand::thread_rng().gen_range(lo..(hi + 1));
            let key = vec![i, i16::MAX - i, i16::MAX - 2 * i];

            let present = view.contains(&key).await?;

            assert_eq!(present, view.delete(key.to_vec()).await?);
            assert!(!view.contains(&key).await?);

            if present {
                #[cfg(debug_assertions)]
                assert!(view.is_valid().await?);
                count -= 1;
            }

            assert_eq!(view.count(&Range::default()).await?, count);
        }

        assert_eq!(
            view.into_stream(Range::default(), false).try_next().await?,
            None
        );
    }

    // clean up
    fs::remove_dir_all(path).await
}

async fn load_test() -> Result<(), io::Error> {
    let n = 100_000;

    // set up the test directory
    let path = setup_tmp_dir().await?;

    // construct the schema
    let schema = ExampleSchema::<i16>::new(3);

    // initialize the cache
    let cache = Cache::<File>::new(schema.block_size() * n, None);

    // load the directory and file paths into memory (not file contents, yet)
    let dir = cache.load(path.clone())?;

    // create a new B+ tree
    let btree = BTreeLock::create(schema, Collator::<i16>::default(), dir)?;

    {
        let mut view = btree.write().await;

        assert!(view.is_empty(&Range::default()).await?);
        assert_eq!(view.count(&Range::default()).await?, 0);

        for _ in 0..(n / 2) {
            let i: i16 = rand::thread_rng().gen_range(i16::MIN..i16::MAX);
            let key = vec![i, i / 2, i % 2];
            view.insert(key).await?;
        }

        for _ in (n / 2)..n {
            let i: i16 = rand::thread_rng().gen_range(i16::MIN..i16::MAX);
            let key = vec![i, i / 2, i % 2];
            view.insert(key).await?;

            let i: i16 = rand::thread_rng().gen_range(i16::MIN..i16::MAX);
            let key = vec![i, i / 2, i % 2];
            view.delete(key).await?;
        }

        for _ in 0..(n / 2) {
            let i: i16 = rand::thread_rng().gen_range(i16::MIN..i16::MAX);
            let key = vec![i, i / 2, i % 2];
            view.delete(key).await?;
        }
    }

    // clean up
    fs::remove_dir_all(path).await
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    load_test().await?;
    functional_test().await?;
    Ok(())
}

async fn count_keys(
    view: &BTreeReadGuard<ExampleSchema<i16>, Collator<i16>, File>,
    range: &Range<i16>,
) -> Result<u64, io::Error> {
    let mut count = 0u64;

    let mut leaves = view.to_stream(range);
    while let Some(leaf) = leaves.try_next().await? {
        count += leaf.len() as u64;
    }

    Ok(count)
}
