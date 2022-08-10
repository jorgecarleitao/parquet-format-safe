pushd ../thrift/compiler/cpp/
make
popd

../thrift/compiler/cpp/bin/thrift --gen rs parquet.thrift
mv parquet.rs src/parquet_format.rs
