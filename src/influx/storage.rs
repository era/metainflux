use anyhow::{Context, Result};

use parquet::data_type::ByteArrayType;
use parquet::file::writer::SerializedRowGroupWriter;
use parquet::{
    column::writer::ColumnWriter,
    data_type::ByteArray,
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::{fs, path::Path, sync::Arc};

pub static tags_schema: &str = "
            message schema {
                REQUIRED BINARY measurement (UTF8);
                REQUIRED BINARY tag (UTF8);
                REQUIRED BINARY value (UTF8);
            }
        ";

pub static fields_schema: &str = "
            message schema {
                REQUIRED BINARY measurement (UTF8);
                REQUIRED BINARY value (UTF8);
            }
        ";
fn write_bytearray<E, W: std::io::Write>(
    row_group_writer: &mut SerializedRowGroupWriter<W>,
    val: E,
) -> Result<()>
where
    parquet::data_type::ByteArray: From<E>,
{
    let data_writer = row_group_writer.next_column().unwrap();
    if let Some(mut writer) = data_writer {
        let typed = writer.typed::<ByteArrayType>();

        let values = ByteArray::from(val);
        typed.write_batch(&[values], None, None).unwrap();

        writer.close().unwrap();
    }

    Ok(())
}
pub fn write_tag_file(
    path: &PathBuf,
    measurement: &str,
    tags: &HashMap<String, Vec<String>>,
) -> Result<()> {
    let schema = Arc::new(parse_message_type(tags_schema).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .context("could not open or create file")?;
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();

    for (key, values) in tags {
        for value in values {
            let mut row_group_writer = writer.next_row_group().unwrap();

            write_bytearray::<&str, std::fs::File>(&mut row_group_writer, measurement).unwrap();
            write_bytearray::<&str, std::fs::File>(&mut row_group_writer, key.as_str()).unwrap();
            write_bytearray::<&str, std::fs::File>(&mut row_group_writer, value.as_str()).unwrap();

            row_group_writer.close().unwrap();
        }
    }

    writer.close().unwrap();
    Ok(())
}

pub fn write_field_file(path: &PathBuf, measurement: &str, fields: &Vec<String>) -> Result<()> {
    let schema = Arc::new(parse_message_type(fields_schema).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let file = fs::File::create(&path).unwrap(); //TODO should not always just create
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();

    for val in fields {
        let mut row_group_writer = writer.next_row_group().unwrap();
        write_bytearray::<&str, std::fs::File>(&mut row_group_writer, measurement).unwrap();
        write_bytearray::<&str, std::fs::File>(&mut row_group_writer, val.as_str()).unwrap();

        row_group_writer.close().unwrap();
    }

    writer.close().unwrap();
    Ok(())
}
