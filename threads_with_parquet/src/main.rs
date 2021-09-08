
// use indexed_line_reader::*;
// use rayon::prelude::*;
use std::env;
// use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use std::thread;
// use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::time::Instant;
// use parquet::basic::Type as PhysicalType;
use parquet::file::reader::{FileReader, SerializedFileReader};
// use parquet::record::Field;
// use parquet::record::{Row, RowAccessor};
use parquet::schema::types::Type;
use parquet::column::reader::ColumnReaderImpl;
use parquet::data_type::{Int64Type};
use parquet::column::reader::ColumnReader;

pub struct Args {
    pub filename: String, // the parquet file to read (the path)
    pub group_by: Option<String>, // the groupby column to display
    pub operation: Option<String>,    // query to filter the data
    pub column: Option<String>,   // column to apply aggreagation on
}

impl Args {
    pub fn new() -> Args {
        Args {
            filename: String::new(),
            group_by: None,
            operation: None,
            column: None,
        }
    }
}

fn main() {
    let start_time = Instant::now(); //starts the time counter

    //---------------------------ARGS-------------------------------
    let mut args: Args = Args::new();
    args.filename = env::args().nth(1).expect("Missing file path");
    args.group_by = env::args().nth(2);
    args.operation = env::args().nth(3);
    args.column = env::args().nth(4);
    let filename_thread = Arc::new(String::from(args.filename.clone())); //this serves as a shared ownership variable between threads

    //---------------------------VARIABLES-----------------------------------

    let (reader, _) = read_file(&args.filename);
    let number_of_threads =  reader.metadata().file_metadata().schema().get_fields().len(); // then number of threads is equal to the number of columns/fields

    let vec_storage = Arc::new(Mutex::new(vec![])); // stores the records after reading them


    let counter = Arc::new(Mutex::new(0 as usize)); // this keeps count of the threads we created so far and it is shared between the threads
    let total_num_read_records = Arc::new(Mutex::new(0 as usize)); // this keeps count of the number of read records so far
    let mut handles = vec![]; // this is the vector of handles of all the created threads so that the main waits for them to finish before continuing the execution of the code

    //---------------------------------SPAWNING LOOP------------------------------
    for _ in 0..number_of_threads {
        // looping over the number of threads
        let counter = Arc::clone(&counter); // this is a a clone/reference that points to the same allocation of the counter variable
        let total_num_read_records = Arc::clone(&total_num_read_records);
        let vec_storage = Arc::clone(&vec_storage); // clone of the vector storage
        let filename_thread = Arc::clone(&filename_thread); // clone of the filename


        let handle = thread::spawn(move || 
            // the thread takes ownership of all the variables that is within its scope
            {
            let mut counter_lock = counter.lock().unwrap(); //locking the shared variable counter before modifying it
            let current_column_index = *counter_lock ; //the index of the column that this thread is reading
            *counter_lock += 1; // incrementing the count of the created threads
            drop(counter_lock); //drops the lock over this variable as we do not need it anymore in this scope
            
            
            let (reader, _) = read_file(& *filename_thread);
            let mut column_vec_read=vec![];
            for i in 0 ..reader.num_row_groups() 
            // looping over the chunks of rows that the file is divided into
            {
                let row_group = reader.get_row_group(i).unwrap(); // holding the current group (Chunk)
                let num_rows = row_group.metadata().num_rows(); // gets the number fo rows in this chick to display it
                println!("thread number {} row group {} with number of rows {} column name: {}",current_column_index, i,num_rows,reader.metadata().file_metadata().schema().get_fields()[current_column_index].name());
                 match row_group.get_column_reader(current_column_index).unwrap() 
                 //matching the type of the column reader of the current column
                 {
                    ColumnReader::BoolColumnReader(_) => { println!("Bool"); },
                    ColumnReader::ByteArrayColumnReader(_) => { println!("Byte"); },
                    ColumnReader::DoubleColumnReader(_) => { println!("Double"); },
                    ColumnReader::FixedLenByteArrayColumnReader(_) => { println!("FixedLen"); },
                    ColumnReader::FloatColumnReader(_) => { println!("FLoat"); },
                    ColumnReader::Int32ColumnReader(_) => { println!("Int32"); },
                    ColumnReader::Int64ColumnReader(v) => { 
                        //if the column rader is of type int64
                        println!("Int64");
                        let (column, count) = read_i64(v, num_rows as usize); // where columns is a vector of ints and count is the number of recodrds read
                        column_vec_read.extend(column);
                        let mut total_num_read_records_lock = total_num_read_records.lock().unwrap();
                        *total_num_read_records_lock += count;
                        drop(total_num_read_records_lock);
                    },             
                    ColumnReader::Int96ColumnReader(_) => { 
                        println!("Int96"); 
                    },
                }
            }
                let mut vec_storage_lock = vec_storage.lock().unwrap(); //locking the shared vector
                vec_storage_lock.push(column_vec_read.clone());
                drop(vec_storage_lock); //drops the lock over this variable as we do not need it anymore in this scope
                column_vec_read.clear();
            }
        );
        handles.push(handle); //add the handle of the created thread to the vector
    }
    for handle in handles {
        handle.join().unwrap(); // waits for all the threads to finish
    }

     println!("The number of read record is: {}",*total_num_read_records.lock().unwrap()); // see if we successfully read all the records 
    
    let end_time = start_time.elapsed(); // calculates the elapsed time
    println!("The reading time is: {:?}", end_time);
}

fn read_file(file_path: &str) -> (SerializedFileReader<File>, Type) {
    let file = File::open(&Path::new(file_path)).unwrap(); // opens the file
    //Parquet file reader API. With this, user can get metadata information about the Parquet file, can get reader for each row group,
    //and access record iterator.
    let reader = SerializedFileReader::new(file).unwrap(); 
    let schema = reader.metadata().file_metadata().schema().clone(); //gets metadata information about the Parquet file
    (reader, schema)
}

fn read_i64(mut column_reader: ColumnReaderImpl<Int64Type>, num_rows: usize) -> (Vec<i64>, usize) {
    let mut data = vec![]; // the vector that we shall return holding the data/records that we have read
    const BATCH_SIZE: usize = 100; // we divided the current column into batches
    let mut count = 0; // holds the total number of read records
    for _ in 0..(num_rows as f64 / BATCH_SIZE as f64).ceil() as i64 
    //looping over the number of batches that we divided the number of rows on
    {
        let mut values: [i64; BATCH_SIZE] = [0; BATCH_SIZE]; // values is an array with size batch_size and initialized with zeros
        let (num, _) = column_reader.read_batch(BATCH_SIZE, None, None, &mut values).unwrap(); // num holds the number of read record in this batch
        count += num;
        data.extend(values);//concatenating the current read batch with the previous ones
    }
    (data, count)
}