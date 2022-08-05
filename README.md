# parquet-format-async-temp

This is a crate containing a subset of rust's thirft library and associated generated
parquet.

* supports `async` read API (via `futures`)
* supports `async` write API (via `futures`)
* the write API returns the number of written bytes
* the read API is panic free
* the read API has a bound on the maximum number of possible bytes read, to avoid OOM.

It must be used with the fork of thrift's compiler available
at https://github.com/jorgecarleitao/thrift/tree/write_size .
