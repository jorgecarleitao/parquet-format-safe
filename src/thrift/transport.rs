pub trait TReadTransport: std::io::Read {}

pub trait TWriteTransport: std::io::Write {}

impl<T> TReadTransport for T where T: std::io::Read {}

impl<T> TWriteTransport for T where T: std::io::Write {}
