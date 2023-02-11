use std::io;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use collate::Collator;
use freqfs::{Cache, FileLoad};
use rand::Rng;
use safecast::as_type;
use tokio::fs;

use b_tree::{BTreeLock, Node};

const BLOCK_SIZE: usize = 4_096;

enum File {
    Node(Node<i32>),
}

as_type!(File, Node, Node<i32>);

#[async_trait]
impl FileLoad for File {
    async fn load(
        _path: &Path,
        mut _file: fs::File,
        _metadata: std::fs::Metadata,
    ) -> Result<Self, io::Error> {
        unimplemented!("File::load")
    }

    async fn save(&self, _file: &mut fs::File) -> Result<u64, io::Error> {
        unimplemented!("File::save")
    }
}

struct Schema;

impl b_tree::Schema for Schema {
    type Error = io::Error;
    type Value = i32;

    fn block_size(&self) -> usize {
        BLOCK_SIZE
    }

    fn order(&self) -> usize {
        5
    }

    fn validate(&self, key: Vec<i32>) -> Result<Vec<i32>, io::Error> {
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
    let _btree = BTreeLock::create(Schema, Collator::<i32>::default(), dir);

    // clean up
    fs::remove_dir(path).await
}
