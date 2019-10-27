# lmdb-qbsdiff
An LMDB wrapper that saves a diff for each write transaction commited

It uses [qbsdiff](https://docs.rs/qbsdiff) to save binary patches internally.

## Usage

You first need to rus the master binary that will produce patches/additions or deletion for you in a tmp lmdb.
Once you are done with your entries, press `ctrl-d`. The binary will indicate the path of the patch db.

```bash
$ cargo run --bin master
master: how are you?: is now a little explanation string
saved how are you?:is now a little explanation string
master: what:will you do about that?
saved what:will you do about that?
master:
diff stored at TempDir { path: "/var/folders/md/y4w5zd_92kg_w7qtcwbbpb2r0000gn/T/.tmpnrnAHx" }
waiting for you... (press enter)

```

Once you have produced your patch database, you must run the slave bianry in another window and give it the previous patch db path.
Now you can query the database about the values in the patched database.

```bash
$ cargo run --bin slave -- /var/folders/md/y4w5zd_92kg_w7qtcwbbpb2r0000gn/T/.tmpnrnAHx
seen patch
seen patch
slave: how are you?
is now a little explanation string
slave: what
will you do about that?
slave:
```
