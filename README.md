# lmdb-qbsdiff
An LMDB wrapper that saves a diff for each write transaction commited

It uses [qbsdiff](https://docs.rs/qbsdiff) to save binary patches internally.
