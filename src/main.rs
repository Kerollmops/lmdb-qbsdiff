use std::borrow::Cow;
use std::marker;
use std::ops::Deref;
use std::path::Path;
use std::{fs, io};

use heed::types::{ByteSlice, OwnedType, Str};
use main_error::MainError;
use qbsdiff::Bsdiff;
use rich_diff::{RichDiff, RichCodec};

mod rich_diff;

const ONE_GIGA: usize = 1 * 1024 * 1024 * 1024;

struct DifferEnv {
    tmpdir: tempfile::TempDir,
    diff: heed::Env,
    env: heed::Env,
}

impl DifferEnv {
    pub fn open<P: AsRef<Path>>(path: P) -> heed::Result<DifferEnv> {
        let path = path.as_ref();

        fs::create_dir_all(path)?;
        let env = heed::EnvOpenOptions::new()
            .map_size(ONE_GIGA)
            .max_dbs(3000)
            .open(path)?;

        let tmpdir = tempfile::tempdir()?;
        let diff = heed::EnvOpenOptions::new()
            .map_size(ONE_GIGA)
            .max_dbs(3000)
            .open(&tmpdir)?;

        Ok(DifferEnv { tmpdir, diff, env })
    }

    pub fn create_database<KC, DC>(
        &self,
        name: Option<&str>,
    ) -> heed::Result<DifferDatabase<KC, DC>>
    where
        KC: 'static,
        DC: 'static,
    {
        let db = self.env.create_poly_database(name)?;
        let diff = self.diff.create_database(name)?;

        Ok(DifferDatabase { diff, db, _marker: marker::PhantomData })
    }

    pub fn write_txn(&self) -> heed::Result<DifferRwTxn> {
        let txn = self.env.write_txn()?;
        let diff = self.diff.write_txn()?;

        Ok(DifferRwTxn { diff, txn })
    }
}

struct DifferRwTxn {
    diff: heed::RwTxn,
    txn: heed::RwTxn,
}

impl Deref for DifferRwTxn {
    type Target = heed::RoTxn;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

impl DifferRwTxn {
    pub fn commit(self) -> heed::Result<()> {
        self.diff.commit()?;
        self.txn.commit()?;
        Ok(())
    }
}

struct DifferDatabase<KC, DC> {
    diff: heed::Database<ByteSlice, RichCodec>,
    db: heed::PolyDatabase,
    _marker: marker::PhantomData<(KC, DC)>,
}

impl<KC, DC> DifferDatabase<KC, DC> {
    pub fn put<'a>(
        &self,
        txn: &mut DifferRwTxn,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> heed::Result<()>
    where
        KC: heed::BytesEncode<'a>,
        DC: heed::BytesEncode<'a>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).ok_or(heed::Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(&data).ok_or(heed::Error::Encoding)?;

        match self.db.get::<KC, ByteSlice>(&txn.txn, key)? {
            Some(prev) => {
                let mut patch = Vec::new();
                Bsdiff::new(&prev, &data_bytes).compare(&mut patch)?;
                let rich_diff = RichDiff::Patch(&patch);
                self.diff.put(&mut txn.diff, &key_bytes, &rich_diff)?;
            },
            None => {
                let rich_diff = RichDiff::Addition(&data_bytes);
                self.diff.put(&mut txn.diff, &key_bytes, &rich_diff)?;
            },
        }

        self.db.put::<KC, DC>(&mut txn.txn, key, data)
    }

    pub fn delete<'a>(
        &self,
        txn: &mut DifferRwTxn,
        key: &'a KC::EItem,
    ) -> heed::Result<bool>
    where
        KC: heed::BytesEncode<'a>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).ok_or(heed::Error::Encoding)?;
        let rich_diff = RichDiff::Deletion;
        self.diff.put(&mut txn.diff, &key_bytes, &rich_diff)?;

        self.db.delete::<KC>(&mut txn.txn, key)
    }
}

fn main() -> Result<(), MainError> {
    let _ = fs::remove_dir_all("target/zerocopy.mdb");
    let env = DifferEnv::open("target/zerocopy.mdb")?;

    let db = env.create_database::<Str, OwnedType<i32>>(None)?;

    let mut wtxn = env.write_txn()?;
    db.put(&mut wtxn, "hello", &43)?;
    db.put(&mut wtxn, "bonjour", &42)?;

    db.delete(&mut wtxn, "bonjour")?;

    wtxn.commit()?;

    println!("diff stored at {:?}", env.tmpdir);
    let wtxn = env.write_txn()?;

    for result in db.diff.iter(&wtxn.diff)? {
        println!("{:?}", result);
    }

    drop(wtxn);

    println!("waiting for you... (press enter)");
    io::stdin().read_line(&mut String::new())?;

    Ok(())
}
