#![allow(non_camel_case_types)]
#![forbid(unsafe_code)]

mod parquet_format;
pub use crate::parquet_format::*;

pub mod thrift;

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom};

    use super::*;

    #[test]
    fn basic() {
        let mut writer = std::io::Cursor::new(vec![]);
        let mut protocol = thrift::protocol::TCompactOutputProtocol::new(&mut writer);
        let metadata = FileMetaData {
            version: 0,
            schema: vec![SchemaElement {
                type_: Some(Type::INT32),
                type_length: None,
                repetition_type: Some(FieldRepetitionType::REQUIRED),
                name: "aaa".to_string(),
                num_children: None,
                converted_type: Some(ConvertedType::DATE),
                scale: None,
                precision: None,
                field_id: None,
                logical_type: Some(LogicalType::DATE(Default::default())),
            }],
            num_rows: 0,
            row_groups: vec![RowGroup {
                columns: vec![ColumnChunk {
                    file_path: None,
                    file_offset: 10,
                    meta_data: None,
                    offset_index_offset: None,
                    offset_index_length: None,
                    column_index_offset: None,
                    column_index_length: None,
                    crypto_metadata: None,
                    encrypted_column_metadata: None,
                }],
                total_byte_size: 10,
                num_rows: 10,
                sorting_columns: None,
                file_offset: Some(10),
                total_compressed_size: Some(10),
                ordinal: None,
            }],
            key_value_metadata: None,
            created_by: None,
            column_orders: None,
            encryption_algorithm: None,
            footer_signing_key_metadata: None,
        };
        metadata.write_to_out_protocol(&mut protocol).unwrap();

        writer.seek(SeekFrom::Start(0)).unwrap();

        let mut prot = thrift::protocol::TCompactInputProtocol::new(writer, usize::MAX);
        let result = FileMetaData::read_from_in_protocol(&mut prot).unwrap();
        assert_eq!(result, metadata)
    }
}
