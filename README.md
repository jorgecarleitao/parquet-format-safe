# parquet-format-async-temp

This is a temporary crate containing a subset of rust's thirft library and parquet
to support native async parquet read and write.

Specifically, it:

* supports `async` read API (via `futures`)
* supports `async` write API (via `futures`)
* the write API returns the number of written bytes

It must be used with the fork of thrift's compiler available
at https://github.com/jorgecarleitao/thrift/tree/write_size .

## Why

To read and write files with thrift (e.g. parquet) without commiting to a
particular runtime (e.g. tokio, hyper, etc.), the protocol needs to support
`AsyncRead + AsyncSeek` and `AsyncWrite` respectively.

To not require `Seek` and `AsyncSeek` on write, the protocol must
return the number of written bytes on its `write_*` API.

This crate addresses these two concerns for parquet. It is essentially:
* https://github.com/apache/thrift/pull/2426 applied on latest thrift
* a modification for the written bytes to be outputed on all `write_*` APIs
