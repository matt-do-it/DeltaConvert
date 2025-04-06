use std::ffi::CStr;
use std::fs::File;
use std::sync::Arc;

use arrow::array::RecordBatchReader;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;

use deltalake::writer::DeltaWriter;
use deltalake::DeltaOps;

#[derive(Debug)]
struct ConvertError;

pub struct DeltaHandle {
    writer: deltalake::writer::RecordBatchWriter,
}

fn create_arrow_record_batch() -> RecordBatch {
    let schema = ArrowSchema::new(vec![
        Field::new("group", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("date", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
    ]);

    // Let string builder
    let mut group_builder = arrow::array::StringBuilder::new();
    let mut value_builder = arrow::array::Int64Builder::new();
    let mut date_builder = arrow::array::StringBuilder::new();
    let mut url_builder = arrow::array::StringBuilder::new();

    for _n in 0..100 {
        group_builder.append_value("Group");
        value_builder.append_value(10000);
        date_builder.append_value("2025-01-01");
        url_builder.append_value("http://www.test.de");
    }

    let group_array = group_builder.finish();
    let value_array = value_builder.finish();
    let date_array = date_builder.finish();
    let url_array = url_builder.finish();

    // Create the record batch using the schema and data columns
    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(group_array) as Arc<dyn arrow::array::Array>,
            Arc::new(value_array) as Arc<dyn arrow::array::Array>,
            Arc::new(date_array) as Arc<dyn arrow::array::Array>,
            Arc::new(url_array) as Arc<dyn arrow::array::Array>,
        ],
    )
    .expect("Failed to create record batch");

    record_batch
}

fn create_arrow_record_batch_reader() -> impl arrow::array::RecordBatchReader {
    let mut batches: Vec<RecordBatch> = vec![];

    for _i in 0..10 {
        batches.push(create_arrow_record_batch())
    }

    let schema = batches[0].schema();

    let reader = arrow::array::RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

    reader
}

async fn write_parquet(
    name: &str,
    record_batch_reader: &mut dyn arrow::array::RecordBatchReader,
) -> Result<(), ConvertError> {
    let file = std::fs::File::create(name).map_err(|_| ConvertError)?;

    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, record_batch_reader.schema(), Some(props))
            .unwrap();

    for batch in record_batch_reader {
        let _result = writer.write(&batch.unwrap()).expect("Writing batch");
    }

    writer.close().unwrap();

    Ok(())
}

async fn parquet_record_batch_reader(
    name: &str,
) -> Result<parquet::arrow::arrow_reader::ParquetRecordBatchReader, ConvertError> {
    let file = File::open(name).unwrap();

    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

    builder.build().map_err(|_| ConvertError)
}

// We are just initing, as we take the first record batch to get the schema
async fn init_delta_table_rust(
    name: &str,
    arrow_schema: &ArrowSchema,
) -> Result<DeltaHandle, ConvertError> {
    let delta_schema =
        deltalake::kernel::Schema::try_from(arrow_schema).map_err(|_| ConvertError)?;

    let ops = DeltaOps::try_from_uri(name).await.unwrap();

    let delta_table = ops
        .create()
        .with_columns(delta_schema.fields().into_iter().cloned())
        .await
        .map_err(|_| ConvertError)?;

    let writer =
        deltalake::writer::RecordBatchWriter::for_table(&delta_table).map_err(|_| ConvertError)?;

    Ok(DeltaHandle { writer: writer })
}

#[no_mangle]
pub extern "C" fn init_delta_table(
    name: *const i8,
    schema: &deltalake::arrow::array::ffi::FFI_ArrowSchema,
) -> *mut DeltaHandle {
    unsafe {
        let c_name = CStr::from_ptr(name).to_str();
        let c_name_str = c_name.unwrap_unchecked();

        let rust_schema = ArrowSchema::try_from(schema);
        if !rust_schema.is_ok() {
            return std::ptr::null_mut();
        }

        let result = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(init_delta_table_rust(
                &c_name_str,
                &rust_schema.unwrap_unchecked(),
            ));

        if !result.is_ok() {
            return std::ptr::null_mut();
        }

        let boxed = Box::new(result.unwrap_unchecked());
        Box::into_raw(boxed)
    }
}

// Read an IPC buffer
async fn write_delta_table_rust(
    handle: &mut DeltaHandle,
    in_data: *const u8,
    in_size: usize,
) -> Result<(), ConvertError> {
    unsafe {
        let data_slice = std::slice::from_raw_parts(in_data, in_size);
        let data_vec = data_slice.to_vec();

        let cursor = std::io::Cursor::new(data_vec);
        let reader =
            arrow::ipc::reader::StreamReader::try_new(cursor, None).map_err(|_| ConvertError)?;

        for result_batch in reader {
            let result_record_batch = result_batch.map_err(|_| ConvertError)?;
            let _result = handle
                .writer
                .write(result_record_batch)
                .await
                .map_err(|_| ConvertError)?;
        }
    }
    Ok(())
}

#[no_mangle]
pub extern "C" fn write_delta_table(
    handle: *mut DeltaHandle,
    in_data: *const u8,
    in_size: usize,
) -> i32 {
    unsafe {
        let result = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(write_delta_table_rust(&mut *handle, in_data, in_size));

        if !result.is_ok() {
            return -1;
        }

        return 0;
    }
}

async fn close_delta_table_rust(handle: &mut DeltaHandle) -> Result<(), ConvertError> {
    handle.writer.flush().await.map_err(|_| ConvertError)?;

    Ok(())
}

#[no_mangle]
pub extern "C" fn close_delta_table(handle: *mut DeltaHandle) -> i32 {
    if !handle.is_null() {
        unsafe {
            let result = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(close_delta_table_rust(&mut *handle));

            if !result.is_ok() {
                drop(Box::from_raw(handle)); // This will drop the Box, freeing the memory
                return -1;
            } else {
                return 0;
            }
        }
    } else {
        return -1;
    }
}

#[tokio::main]
async fn main() -> Result<(), ConvertError> {
    //
    // Create sample arrow reader
    //
    let mut sample_record_batch_reader = create_arrow_record_batch_reader();

    //
    // Write example parquet file
    //
    let _ = write_parquet("test1.parquet", &mut sample_record_batch_reader).await;

    //
    // Create parquet reader
    //
    let file_reader = parquet_record_batch_reader("test1.parquet").await?;

    //
    // Create delta handle
    //
    let mut handle = init_delta_table_rust("delta", &file_reader.schema()).await?;

    //
    // Read batches from reader and write them to delta
    //
    for batch in file_reader {
        let record_batch = batch.map_err(|_| ConvertError)?;
        let mut buffer = Vec::new();
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut buffer, &record_batch.schema())
                .map_err(|_| ConvertError)?;

        writer.write(&record_batch).unwrap();
        writer.finish().unwrap();

        write_delta_table_rust(&mut handle, buffer.as_ptr(), buffer.len()).await?;
    }

    //
    // Close delta handle
    //
    close_delta_table_rust(&mut handle).await?;

    Ok(())
}
