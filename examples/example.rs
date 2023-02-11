use std::io;
use std::path::{Path, PathBuf};

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

use b_tree::{BTreeLock, Node};

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
                .map_ok(Node::Index)
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
                Node::Index(children) => (false, children).into_stream(encoder),
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

        tokio::io::copy(&mut reader, file).await
    }
}

struct Schema;

impl b_tree::Schema for Schema {
    type Error = io::Error;
    type Value = i16;

    fn block_size(&self) -> usize {
        BLOCK_SIZE
    }

    fn order(&self) -> usize {
        5
    }

    fn validate(&self, key: Vec<i16>) -> Result<Vec<i16>, io::Error> {
        if key.len() == 3 {
            Ok(key)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "key length should be 3",
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

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    // set up the test directory
    let path = setup_tmp_dir().await?;

    // initialize the cache
    let cache = Cache::<File>::new(40, None);

    // load the directory and file paths into memory (not file contents, yet)
    let dir = cache.load(path.clone())?;

    // create a new B+ tree
    let btree = BTreeLock::create(Schema, Collator::default(), dir)?;

    {
        let view = btree.write().await;
        assert!(view.insert(vec![0, i16::MAX, i16::MAX]).await?);
    }

    // clean up
    fs::remove_dir(path).await
}
