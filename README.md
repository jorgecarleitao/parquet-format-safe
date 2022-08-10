# parquet-format-safe

This is a crate contains an implementation of Thirft and generated Rust code
associated to Parquet's thrift definition.

* supports `sync` and `async` read API
* supports `sync` and `async` write API
* the write API returns the number of written bytes
* the read API is panic free
* the read API has a bound on the maximum number of possible bytes read, to avoid OOM.

It must be used with the fork of thrift's compiler available
at https://github.com/jorgecarleitao/thrift/tree/safe .
