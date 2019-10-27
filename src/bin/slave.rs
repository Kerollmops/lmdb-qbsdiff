use std::{fs, io, env};

use heed::types::ByteSlice;
use lmdb_qbsdiff::{ONE_GIGA, RichDiff, RichCodec};
use main_error::MainError;
use qbsdiff::Bspatch;

fn bspatch(source: &[u8], patch: &[u8]) -> io::Result<Vec<u8>> {
    let patcher = Bspatch::new(patch)?;
    let mut target = Vec::with_capacity(patcher.hint_target_size() as usize);
    patcher.apply(source, io::Cursor::new(&mut target))?;
    Ok(target)
}

fn main() -> Result<(), MainError> {
    let patch_path = env::args().nth(1).unwrap();
    let patch_env = heed::EnvOpenOptions::new().map_size(ONE_GIGA).max_dbs(3000).open(patch_path)?;

    fs::create_dir_all("target/slave.mdb")?;
    let env = heed::EnvOpenOptions::new()
        .map_size(ONE_GIGA)
        .max_dbs(3000)
        .open("target/slave.mdb")?;

    let patch_db = patch_env.open_database::<ByteSlice, RichCodec>(None)?.unwrap();
    let db = env.create_database::<ByteSlice, ByteSlice>(None)?;

    let patch_rtxn = patch_env.read_txn()?;
    let mut wtxn = env.write_txn()?;

    for result in patch_db.iter(&patch_rtxn)? {
        match result? {
            (key, RichDiff::Addition(bytes)) => {
                println!("seen add");
                db.put(&mut wtxn, key, bytes)?;
            },
            (key, RichDiff::Patch(bytes)) => {
                println!("seen patch");
                let prev = db.get(&wtxn, key)?.expect("the key to exist because its a patch");
                let bytes = bspatch(prev, bytes)?;
                db.put(&mut wtxn, key, &bytes)?;
            },
            (key, RichDiff::Deletion) => {
                println!("seen del");
                db.delete(&mut wtxn, key)?;
            },
        }
    }

    wtxn.commit()?;

    let rtxn = env.read_txn()?;

    let mut rl = rustyline::Editor::<()>::new();
    for result in rl.iter("lmdb-qbsdiff: ") {
        let line = result?;
        let key = line.trim();

        match db.get(&rtxn, key.as_bytes())? {
            Some(bytes) => {
                let string = std::str::from_utf8(bytes)?;
                println!("{}", string);
            },
            None => println!("None"),
        }
    }

    Ok(())
}
