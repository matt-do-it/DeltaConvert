use std::sync::Arc;
use std::os::raw::c_char;
use std::ffi::{CStr, CString};

use arrow::datatypes::{Schema as ArrowSchema, Field, DataType};
use arrow::record_batch::RecordBatch;

use deltalake::{DeltaTableError};
use deltalake::writer::DeltaWriter;
use deltalake::DeltaOps;

fn create_arrow_record_batch() -> RecordBatch {
    let schema = ArrowSchema::new(vec![
        Field::new("group", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("date", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false)
    ]);

	// Let string builder
	let mut group_builder = arrow::array::StringBuilder::new();
	let mut value_builder = arrow::array::Int64Builder::new();
	let mut date_builder = arrow::array::StringBuilder::new();
	let mut url_builder = arrow::array::StringBuilder::new();

	for _n in 0..100 {
		group_builder.append_value("Group");
		value_builder.append_value(10000);
		date_builder.append_value("Group");
		url_builder.append_value("Group");
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
            Arc::new(url_array) as Arc<dyn arrow::array::Array>
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
   
pub extern "C" fn convert_delta_extern(
		name: *const c_char, 
		record_batch_reader: &mut dyn arrow::array::RecordBatchReader) 
	-> i32 {
	
	unsafe {
    	let name_str_result = CStr::from_ptr(name).to_str();
    	if !name_str_result.is_ok() {
    		return -1;
    	}
    	let name_str = name_str_result.unwrap_unchecked();
    
		let result = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(
            convert_delta_async(name_str, record_batch_reader)
        );
        if (!result.is_ok()) {
        	return -2; 
        }
    }    

	return 0;
}

async fn convert_delta_async(
		name: &str, 
		record_batch_reader: &mut dyn arrow::array::RecordBatchReader) 
	-> Result<(), DeltaTableError> {
	let arrow_schema = record_batch_reader.schema(); 
	let delta_schema = deltalake::kernel::Schema::try_from(arrow_schema)?;

	let ops = DeltaOps::try_from_uri(name).await.unwrap();

	let delta_table = ops.create()
		.with_columns(delta_schema.fields().into_iter().cloned())
		.await?;
	
	let mut writer = deltalake::writer::RecordBatchWriter::for_table(&delta_table)?;
	
	for batch in record_batch_reader {
		let _result = writer.write(batch?).await?;
 	}

	writer.flush().await?;

    Ok(())
}

fn main() -> Result<(), i32> {
	let mut record_batch_reader = create_arrow_record_batch_reader();

	let c_name = CString::new("delta4").expect("CString::new failed");
	
    
	let status_code = convert_delta_extern(c_name.as_ptr(), &mut record_batch_reader);
	
	if status_code == 0 {
        println!("Program ran successfully.");
        Ok(())  // Return success with Ok(())
    } else {
        eprintln!("Error occurred.");
        Err(status_code)  // Return error with Err(status_code)
    }
}