// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use integer_encoding::{VarIntReader, VarIntWriter};
use std::convert::{From, TryFrom};
use std::io;

use super::super::transport::{TReadTransport, TWriteTransport};
use super::super::{Error, ProtocolError, ProtocolErrorKind, Result};
use super::{
    TFieldIdentifier, TInputProtocol, TInputProtocolFactory, TListIdentifier, TMapIdentifier,
    TMessageIdentifier, TMessageType,
};
use super::{TOutputProtocol, TOutputProtocolFactory, TSetIdentifier, TStructIdentifier, TType};

pub(super) const COMPACT_PROTOCOL_ID: u8 = 0x82;
pub(super) const COMPACT_VERSION: u8 = 0x01;
pub(super) const COMPACT_VERSION_MASK: u8 = 0x1F;

/// Read messages encoded in the Thrift compact protocol.
///
/// # Examples
///
/// Create and use a `TCompactInputProtocol`.
///
/// ```no_run
/// use thrift::protocol::{TCompactInputProtocol, TInputProtocol};
/// use thrift::transport::TTcpChannel;
///
/// let mut channel = TTcpChannel::new();
/// channel.open("localhost:9090").unwrap();
///
/// let mut protocol = TCompactInputProtocol::new(channel);
///
/// let recvd_bool = protocol.read_bool().unwrap();
/// let recvd_string = protocol.read_string().unwrap();
/// ```
#[derive(Debug)]
pub struct TCompactInputProtocol<T>
where
    T: TReadTransport,
{
    // Identifier of the last field deserialized for a struct.
    last_read_field_id: i16,
    // Stack of the last read field ids (a new entry is added each time a nested struct is read).
    read_field_id_stack: Vec<i16>,
    // Boolean value for a field.
    // Saved because boolean fields and their value are encoded in a single byte,
    // and reading the field only occurs after the field id is read.
    pending_read_bool_value: Option<bool>,
    // Underlying transport used for byte-level operations.
    transport: T,
}

impl<T> TCompactInputProtocol<T>
where
    T: TReadTransport,
{
    /// Create a `TCompactInputProtocol` that reads bytes from `transport`.
    pub fn new(transport: T) -> TCompactInputProtocol<T> {
        TCompactInputProtocol {
            last_read_field_id: 0,
            read_field_id_stack: Vec::new(),
            pending_read_bool_value: None,
            transport,
        }
    }

    fn read_list_set_begin(&mut self) -> Result<(TType, i32)> {
        let header = self.read_byte()?;
        let element_type = collection_u8_to_type(header & 0x0F)?;

        let element_count;
        let possible_element_count = (header & 0xF0) >> 4;
        if possible_element_count != 15 {
            // high bits set high if count and type encoded separately
            element_count = possible_element_count as i32;
        } else {
            element_count = self.transport.read_varint::<u32>()? as i32;
        }

        Ok((element_type, element_count))
    }
}

impl<T> TInputProtocol for TCompactInputProtocol<T>
where
    T: TReadTransport,
{
    fn read_message_begin(&mut self) -> Result<TMessageIdentifier> {
        let compact_id = self.read_byte()?;
        if compact_id != COMPACT_PROTOCOL_ID {
            Err(Error::Protocol(ProtocolError {
                kind: ProtocolErrorKind::BadVersion,
                message: format!("invalid compact protocol header {:?}", compact_id),
            }))
        } else {
            Ok(())
        }?;

        let type_and_byte = self.read_byte()?;
        let received_version = type_and_byte & COMPACT_VERSION_MASK;
        if received_version != COMPACT_VERSION {
            Err(Error::Protocol(ProtocolError {
                kind: ProtocolErrorKind::BadVersion,
                message: format!(
                    "cannot process compact protocol version {:?}",
                    received_version
                ),
            }))
        } else {
            Ok(())
        }?;

        // NOTE: unsigned right shift will pad with 0s
        let message_type: TMessageType = TMessageType::try_from(type_and_byte >> 5)?;
        // writing side wrote signed sequence number as u32 to avoid zigzag encoding
        let sequence_number = self.transport.read_varint::<u32>()? as i32;
        let service_call_name = self.read_string()?;

        self.last_read_field_id = 0;

        Ok(TMessageIdentifier::new(
            service_call_name,
            message_type,
            sequence_number,
        ))
    }

    fn read_message_end(&mut self) -> Result<()> {
        Ok(())
    }

    fn read_struct_begin(&mut self) -> Result<Option<TStructIdentifier>> {
        self.read_field_id_stack.push(self.last_read_field_id);
        self.last_read_field_id = 0;
        Ok(None)
    }

    fn read_struct_end(&mut self) -> Result<()> {
        self.last_read_field_id = self
            .read_field_id_stack
            .pop()
            .expect("should have previous field ids");
        Ok(())
    }

    fn read_field_begin(&mut self) -> Result<TFieldIdentifier> {
        // we can read at least one byte, which is:
        // - the type
        // - the field delta and the type
        let field_type = self.read_byte()?;
        let field_delta = (field_type & 0xF0) >> 4;
        let field_type = match field_type & 0x0F {
            0x01 => {
                self.pending_read_bool_value = Some(true);
                Ok(TType::Bool)
            }
            0x02 => {
                self.pending_read_bool_value = Some(false);
                Ok(TType::Bool)
            }
            ttu8 => u8_to_type(ttu8),
        }?;

        match field_type {
            TType::Stop => Ok(
                TFieldIdentifier::new::<Option<String>, String, Option<i16>>(
                    None,
                    TType::Stop,
                    None,
                ),
            ),
            _ => {
                if field_delta != 0 {
                    self.last_read_field_id += field_delta as i16;
                } else {
                    self.last_read_field_id = self.read_i16()?;
                };

                Ok(TFieldIdentifier {
                    name: None,
                    field_type,
                    id: Some(self.last_read_field_id),
                })
            }
        }
    }

    fn read_field_end(&mut self) -> Result<()> {
        Ok(())
    }

    fn read_bool(&mut self) -> Result<bool> {
        match self.pending_read_bool_value.take() {
            Some(b) => Ok(b),
            None => {
                let b = self.read_byte()?;
                match b {
                    0x01 => Ok(true),
                    0x02 => Ok(false),
                    unkn => Err(Error::Protocol(ProtocolError {
                        kind: ProtocolErrorKind::InvalidData,
                        message: format!("cannot convert {} into bool", unkn),
                    })),
                }
            }
        }
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.transport.read_varint::<u32>()?;
        let mut buf = vec![0u8; len as usize];
        self.transport
            .read_exact(&mut buf)
            .map_err(From::from)
            .map(|_| buf)
    }

    fn read_i8(&mut self) -> Result<i8> {
        self.read_byte().map(|i| i as i8)
    }

    fn read_i16(&mut self) -> Result<i16> {
        self.transport.read_varint::<i16>().map_err(From::from)
    }

    fn read_i32(&mut self) -> Result<i32> {
        self.transport.read_varint::<i32>().map_err(From::from)
    }

    fn read_i64(&mut self) -> Result<i64> {
        self.transport.read_varint::<i64>().map_err(From::from)
    }

    fn read_double(&mut self) -> Result<f64> {
        self.transport
            .read_f64::<LittleEndian>()
            .map_err(From::from)
    }

    fn read_string(&mut self) -> Result<String> {
        let bytes = self.read_bytes()?;
        String::from_utf8(bytes).map_err(From::from)
    }

    fn read_list_begin(&mut self) -> Result<TListIdentifier> {
        let (element_type, element_count) = self.read_list_set_begin()?;
        Ok(TListIdentifier::new(element_type, element_count))
    }

    fn read_list_end(&mut self) -> Result<()> {
        Ok(())
    }

    fn read_set_begin(&mut self) -> Result<TSetIdentifier> {
        let (element_type, element_count) = self.read_list_set_begin()?;
        Ok(TSetIdentifier::new(element_type, element_count))
    }

    fn read_set_end(&mut self) -> Result<()> {
        Ok(())
    }

    fn read_map_begin(&mut self) -> Result<TMapIdentifier> {
        let element_count = self.transport.read_varint::<u32>()? as i32;
        if element_count == 0 {
            Ok(TMapIdentifier::new(None, None, 0))
        } else {
            let type_header = self.read_byte()?;
            let key_type = collection_u8_to_type((type_header & 0xF0) >> 4)?;
            let val_type = collection_u8_to_type(type_header & 0x0F)?;
            Ok(TMapIdentifier::new(key_type, val_type, element_count))
        }
    }

    fn read_map_end(&mut self) -> Result<()> {
        Ok(())
    }

    // utility
    //

    fn read_byte(&mut self) -> Result<u8> {
        let mut buf = [0u8; 1];
        self.transport
            .read_exact(&mut buf)
            .map_err(From::from)
            .map(|_| buf[0])
    }
}

impl<T> io::Seek for TCompactInputProtocol<T>
where
    T: io::Seek + TReadTransport,
{
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.transport.seek(pos)
    }
}

/// Factory for creating instances of `TCompactInputProtocol`.
#[derive(Default)]
pub struct TCompactInputProtocolFactory;

impl TCompactInputProtocolFactory {
    /// Create a `TCompactInputProtocolFactory`.
    pub fn new() -> TCompactInputProtocolFactory {
        TCompactInputProtocolFactory {}
    }
}

impl TInputProtocolFactory for TCompactInputProtocolFactory {
    fn create(&self, transport: Box<dyn TReadTransport + Send>) -> Box<dyn TInputProtocol + Send> {
        Box::new(TCompactInputProtocol::new(transport))
    }
}

/// Write messages using the Thrift compact protocol.
///
/// # Examples
///
/// Create and use a `TCompactOutputProtocol`.
///
/// ```no_run
/// use thrift::protocol::{TCompactOutputProtocol, TOutputProtocol};
/// use thrift::transport::TTcpChannel;
///
/// let mut channel = TTcpChannel::new();
/// channel.open("localhost:9090").unwrap();
///
/// let mut protocol = TCompactOutputProtocol::new(channel);
///
/// protocol.write_bool(true).unwrap();
/// protocol.write_string("test_string").unwrap();
/// ```
#[derive(Debug)]
pub struct TCompactOutputProtocol<T>
where
    T: TWriteTransport,
{
    // Identifier of the last field serialized for a struct.
    last_write_field_id: i16,
    // Stack of the last written field ids (new entry added each time a nested struct is written).
    write_field_id_stack: Vec<i16>,
    // Field identifier of the boolean field to be written.
    // Saved because boolean fields and their value are encoded in a single byte
    pending_write_bool_field_identifier: Option<TFieldIdentifier>,
    // Underlying transport used for byte-level operations.
    transport: T,
}

impl<T> TCompactOutputProtocol<T>
where
    T: TWriteTransport,
{
    /// Create a `TCompactOutputProtocol` that writes bytes to `transport`.
    pub fn new(transport: T) -> TCompactOutputProtocol<T> {
        TCompactOutputProtocol {
            last_write_field_id: 0,
            write_field_id_stack: Vec::new(),
            pending_write_bool_field_identifier: None,
            transport,
        }
    }

    // FIXME: field_type as unconstrained u8 is bad
    fn write_field_header(&mut self, field_type: u8, field_id: i16) -> Result<usize> {
        let mut written = 0;

        let field_delta = field_id - self.last_write_field_id;
        if field_delta > 0 && field_delta < 15 {
            written += self.write_byte(((field_delta as u8) << 4) | field_type)?;
        } else {
            written += self.write_byte(field_type)?;
            written += self.write_i16(field_id)?;
        }
        self.last_write_field_id = field_id;
        Ok(written)
    }

    fn write_list_set_begin(&mut self, element_type: TType, element_count: i32) -> Result<usize> {
        let mut written = 0;

        let elem_identifier = collection_type_to_u8(element_type);
        if element_count <= 14 {
            let header = (element_count as u8) << 4 | elem_identifier;
            written += self.write_byte(header)?;
        } else {
            let header = 0xF0 | elem_identifier;
            written += self.write_byte(header)?;
            // element count is strictly positive as per the spec, so
            // cast i32 as u32 so that varint writing won't use zigzag encoding
            written += self.transport.write_varint(element_count as u32)?;
        }
        Ok(written)
    }

    fn assert_no_pending_bool_write(&self) {
        if let Some(ref f) = self.pending_write_bool_field_identifier {
            panic!("pending bool field {:?} not written", f)
        }
    }
}

impl<T> TOutputProtocol for TCompactOutputProtocol<T>
where
    T: TWriteTransport,
{
    fn write_message_begin(&mut self, identifier: &TMessageIdentifier) -> Result<usize> {
        let mut written = 0;
        written += self.write_byte(COMPACT_PROTOCOL_ID)?;
        written += self.write_byte((u8::from(identifier.message_type) << 5) | COMPACT_VERSION)?;
        // cast i32 as u32 so that varint writing won't use zigzag encoding
        written += self
            .transport
            .write_varint(identifier.sequence_number as u32)?;
        written += self.write_string(&identifier.name)?;
        Ok(written)
    }

    fn write_message_end(&mut self) -> Result<usize> {
        self.assert_no_pending_bool_write();
        Ok(0)
    }

    fn write_struct_begin(&mut self, _: &TStructIdentifier) -> Result<usize> {
        self.write_field_id_stack.push(self.last_write_field_id);
        self.last_write_field_id = 0;
        Ok(0)
    }

    fn write_struct_end(&mut self) -> Result<usize> {
        self.assert_no_pending_bool_write();
        self.last_write_field_id = self
            .write_field_id_stack
            .pop()
            .expect("should have previous field ids");
        Ok(0)
    }

    fn write_field_begin(&mut self, identifier: &TFieldIdentifier) -> Result<usize> {
        match identifier.field_type {
            TType::Bool => {
                if self.pending_write_bool_field_identifier.is_some() {
                    panic!(
                        "should not have a pending bool while writing another bool with id: \
                         {:?}",
                        identifier
                    )
                }
                self.pending_write_bool_field_identifier = Some(identifier.clone());
                Ok(0)
            }
            _ => {
                let field_type = type_to_u8(identifier.field_type);
                let field_id = identifier.id.expect("non-stop field should have field id");
                self.write_field_header(field_type, field_id)
            }
        }
    }

    fn write_field_end(&mut self) -> Result<usize> {
        self.assert_no_pending_bool_write();
        Ok(0)
    }

    fn write_field_stop(&mut self) -> Result<usize> {
        self.assert_no_pending_bool_write();
        self.write_byte(type_to_u8(TType::Stop))
    }

    fn write_bool(&mut self, b: bool) -> Result<usize> {
        match self.pending_write_bool_field_identifier.take() {
            Some(pending) => {
                let field_id = pending.id.expect("bool field should have a field id");
                let field_type_as_u8 = if b { 0x01 } else { 0x02 };
                self.write_field_header(field_type_as_u8, field_id)
            }
            None => {
                if b {
                    self.write_byte(0x01)
                } else {
                    self.write_byte(0x02)
                }
            }
        }
    }

    fn write_bytes(&mut self, b: &[u8]) -> Result<usize> {
        let mut written = 0;
        // length is strictly positive as per the spec, so
        // cast i32 as u32 so that varint writing won't use zigzag encoding
        written += self.transport.write_varint(b.len() as u32)?;
        self.transport.write_all(b)?;
        written += b.len();
        Ok(written)
    }

    fn write_i8(&mut self, i: i8) -> Result<usize> {
        self.write_byte(i as u8)
    }

    fn write_i16(&mut self, i: i16) -> Result<usize> {
        self.transport.write_varint(i).map_err(From::from)
    }

    fn write_i32(&mut self, i: i32) -> Result<usize> {
        self.transport.write_varint(i).map_err(From::from)
    }

    fn write_i64(&mut self, i: i64) -> Result<usize> {
        self.transport.write_varint(i).map_err(From::from)
    }

    fn write_double(&mut self, d: f64) -> Result<usize> {
        self.transport.write_f64::<LittleEndian>(d)?;
        Ok(8)
    }

    fn write_string(&mut self, s: &str) -> Result<usize> {
        self.write_bytes(s.as_bytes())
    }

    fn write_list_begin(&mut self, identifier: &TListIdentifier) -> Result<usize> {
        self.write_list_set_begin(identifier.element_type, identifier.size)
    }

    fn write_list_end(&mut self) -> Result<usize> {
        Ok(0)
    }

    fn write_set_begin(&mut self, identifier: &TSetIdentifier) -> Result<usize> {
        self.write_list_set_begin(identifier.element_type, identifier.size)
    }

    fn write_set_end(&mut self) -> Result<usize> {
        Ok(0)
    }

    fn write_map_begin(&mut self, identifier: &TMapIdentifier) -> Result<usize> {
        if identifier.size == 0 {
            self.write_byte(0)
        } else {
            let mut written = 0;
            // element count is strictly positive as per the spec, so
            // cast i32 as u32 so that varint writing won't use zigzag encoding
            written += self.transport.write_varint(identifier.size as u32)?;

            let key_type = identifier
                .key_type
                .expect("map identifier to write should contain key type");
            let key_type_byte = collection_type_to_u8(key_type) << 4;

            let val_type = identifier
                .value_type
                .expect("map identifier to write should contain value type");
            let val_type_byte = collection_type_to_u8(val_type);

            let map_type_header = key_type_byte | val_type_byte;
            written += self.write_byte(map_type_header)?;
            Ok(written)
        }
    }

    fn write_map_end(&mut self) -> Result<usize> {
        Ok(0)
    }

    fn flush(&mut self) -> Result<()> {
        self.transport.flush().map_err(From::from)
    }

    // utility
    //

    fn write_byte(&mut self, b: u8) -> Result<usize> {
        self.transport.write(&[b]).map_err(From::from)
    }
}

/// Factory for creating instances of `TCompactOutputProtocol`.
#[derive(Default)]
pub struct TCompactOutputProtocolFactory;

impl TCompactOutputProtocolFactory {
    /// Create a `TCompactOutputProtocolFactory`.
    pub fn new() -> TCompactOutputProtocolFactory {
        TCompactOutputProtocolFactory {}
    }
}

impl TOutputProtocolFactory for TCompactOutputProtocolFactory {
    fn create(
        &self,
        transport: Box<dyn TWriteTransport + Send>,
    ) -> Box<dyn TOutputProtocol + Send> {
        Box::new(TCompactOutputProtocol::new(transport))
    }
}

pub(super) fn collection_type_to_u8(field_type: TType) -> u8 {
    match field_type {
        TType::Bool => 0x01,
        f => type_to_u8(f),
    }
}

pub(super) fn type_to_u8(field_type: TType) -> u8 {
    match field_type {
        TType::Stop => 0x00,
        TType::I08 => 0x03, // equivalent to TType::Byte
        TType::I16 => 0x04,
        TType::I32 => 0x05,
        TType::I64 => 0x06,
        TType::Double => 0x07,
        TType::String => 0x08,
        TType::List => 0x09,
        TType::Set => 0x0A,
        TType::Map => 0x0B,
        TType::Struct => 0x0C,
        _ => panic!("should not have attempted to convert {} to u8", field_type),
    }
}

pub(super) fn collection_u8_to_type(b: u8) -> Result<TType> {
    match b {
        0x01 => Ok(TType::Bool),
        o => u8_to_type(o),
    }
}

pub(super) fn u8_to_type(b: u8) -> Result<TType> {
    match b {
        0x00 => Ok(TType::Stop),
        0x03 => Ok(TType::I08), // equivalent to TType::Byte
        0x04 => Ok(TType::I16),
        0x05 => Ok(TType::I32),
        0x06 => Ok(TType::I64),
        0x07 => Ok(TType::Double),
        0x08 => Ok(TType::String),
        0x09 => Ok(TType::List),
        0x0A => Ok(TType::Set),
        0x0B => Ok(TType::Map),
        0x0C => Ok(TType::Struct),
        unkn => Err(Error::Protocol(ProtocolError {
            kind: ProtocolErrorKind::InvalidData,
            message: format!("cannot convert {} into TType", unkn),
        })),
    }
}
