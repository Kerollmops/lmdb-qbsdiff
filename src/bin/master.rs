use std::borrow::Cow;
use std::marker;
use std::path::Path;
use std::{fs, io};

use heed::types::{ByteSlice, Str};
use main_error::MainError;
use qbsdiff::Bsdiff;
use lmdb_qbsdiff::{ONE_GIGA, RichDiff, RichCodec};

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
        let diff = self.diff.write_txn()?;
        let rtxn = self.env.read_txn()?;
        let wtxn = self.env.write_txn()?;

        Ok(DifferRwTxn { env: self, diff, rtxn, wtxn })
    }
}

struct DifferRwTxn<'a> {
    env: &'a DifferEnv,
    diff: heed::RwTxn,
    rtxn: heed::RoTxn,
    wtxn: heed::RwTxn,
}

impl DifferRwTxn<'_> {
    pub fn commit(self) -> heed::Result<()> {
        self.rtxn.abort();
        self.diff.commit()?;
        self.wtxn.commit()?;

        println!("diff stored at {:?}", self.env.tmpdir);

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

        match self.db.get::<KC, ByteSlice>(&txn.rtxn, key)? {
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

        self.db.put::<ByteSlice, ByteSlice>(&mut txn.wtxn, &key_bytes, &data_bytes)
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

        self.db.delete::<ByteSlice>(&mut txn.wtxn, &key_bytes)
    }
}

fn main() -> Result<(), MainError> {
    let env = DifferEnv::open("target/master.mdb")?;
    let db = env.create_database::<Str, Str>(None)?;

    let mut wtxn = env.write_txn()?;

    let mut rl = rustyline::Editor::<()>::new();
    for result in rl.iter("master: ") {
        let line = result?;

        let mut iter = line.split(':');
        let key = iter.next().map(|s| s.trim());
        let data = iter.next().map(|s| s.trim());

        match (key, data) {
            (Some(key), Some(data)) => {
                db.put(&mut wtxn, key, data)?;
                println!("saved {}:{}", key, data);
            },
            (Some(key), None) => {
                let deleted = db.delete(&mut wtxn, key)?;
                println!("deleted({}) {}", deleted, key);
            },
            _ => (),
        }
    }

    wtxn.commit()?;

    println!("waiting for you... (press enter)");
    io::stdin().read_line(&mut String::new())?;

    Ok(())
}
