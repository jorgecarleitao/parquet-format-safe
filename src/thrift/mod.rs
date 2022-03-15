pub mod protocol;

mod errors;
pub use errors::*;

pub mod transport;

/// Result type returned by all runtime library functions.
///
/// As is convention this is a typedef of `std::result::Result`
/// with `E` defined as the `thrift::Error` type.
pub type Result<T> = std::result::Result<T, self::Error>;
