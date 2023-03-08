use anyhow::{Context, Result};

use parquet::data_type::ByteArrayType;
use parquet::{
    column::writer::ColumnWriter,
    data_type::ByteArray,
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::{fs, path::Path, sync::Arc};

//TODO add the measurement to the tags schema
// and instead of creating a file per measurement
// just use a single tag file with the measurement column
pub static tags_schema: &str = "
            message schema {
                REQUIRED BINARY name (UTF8);
                REQUIRED BINARY value (UTF8);
            }
        ";

//TODO add the measurement to the fields schema
// and instead of creating a file per measurement
// just use a single fields file with the measurement column
pub static fields_schema: &str = "
            message schema {
                REQUIRED BINARY value (UTF8);
            }
        ";

pub fn write_tag_file(path: &PathBuf, tags: &HashMap<String, Vec<String>>) -> Result<()> {
    let schema = Arc::new(parse_message_type(tags_schema).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let file = fs::File::create(&path).unwrap(); //TODO should not always just create
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();


    for (key, values) in tags {
        for value in values {
            let mut row_group_writer = writer.next_row_group().unwrap();
            {
                let data_writer = row_group_writer.next_column().unwrap();
                if let Some(mut writer) = data_writer {
                    let typed = writer.typed::<ByteArrayType>();

                    let values = ByteArray::from(key.as_str());
                    typed.write_batch(&[values], None, None).unwrap();

                    writer.close().unwrap();
                }
            }

            {
                let data_writer = row_group_writer.next_column().unwrap();
                if let Some(mut writer) = data_writer {
                    let typed = writer.typed::<ByteArrayType>();

                    let values = ByteArray::from(value.as_str());
                    typed.write_batch(&[values], None, None).unwrap();

                    writer.close().unwrap();
                }
            }

            row_group_writer.close().unwrap();
        }

    }

    writer.close().unwrap();
    Ok(())
}

pub fn write_field_file(path: &PathBuf, fields: &Vec<String>) -> Result<()> {
    let schema = Arc::new(parse_message_type(fields_schema).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let file = fs::File::create(&path).unwrap(); //TODO should not always just create
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();

    for val in fields {
        let mut row_group_writer = writer.next_row_group().unwrap();
        {
            let data_writer = row_group_writer.next_column().unwrap();
            if let Some(mut writer) = data_writer {
                let typed = writer.typed::<ByteArrayType>();

                let values = ByteArray::from(val.as_str());
                typed.write_batch(&[values], None, None).unwrap();

                writer.close().unwrap();
            }
        }
        row_group_writer.close().unwrap();
    }

    writer.close().unwrap();
    Ok(())
}
