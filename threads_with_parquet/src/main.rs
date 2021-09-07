
use indexed_line_reader::*;
// use rayon::prelude::*;
use std::env;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use std::thread;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::time::Instant;
use parquet::basic::Type as PhysicalType;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field;
use parquet::record::{Row, RowAccessor};
use parquet::schema::types::Type;

pub struct Args {
    pub filename: String, // the CSV file to read (the path)
    pub number_of_threads: u64,
    pub group_by: Option<String>, // list of columns to display
    pub operation: Option<String>,    // query to filter the data
    pub column: Option<String>,   // column to apply aggreagation on
}

impl Args {
    pub fn new() -> Args {
        Args {
            filename: String::new(),
            number_of_threads: 0,
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
    args.number_of_threads = env::args()
        .nth(2)
        .expect("Missing number of threads")
        .parse()
        .expect("Unable to parse the number of threads");
    args.group_by = env::args().nth(3);
    args.operation = env::args().nth(4);
    args.column = env::args().nth(5);
    let filename_thread = Arc::new(String::from(args.filename.clone())); //this serves as a shared ownership variable between threads

    //---------------------------VARIABLES-----------------------------------

    let (reader, _) = read_file(&args.filename); // data

    // let count_records: u64 = reader.lines().fold(0, |sum, _| sum + 1); // the number of records in the file
    let count_records = reader.metadata().file_metadata().num_rows();// the number of records in the file
    println!("The number of records in the file is: {}", count_records);
    let part_each: u64 = (count_records as f64 / args.number_of_threads as f64).ceil() as u64; //the number of records each thread will take
    println!("part_each = {}",part_each);
    let vec_storage = Arc::new(Mutex::new(vec![])); // stores the records after reading them

    let counter = Arc::new(Mutex::new(0)); // this keeps count of the threads we created so far and it is shared between the threads
    let mut handles = vec![]; // this is the vector of handles of all the created threads so that the main waits for them to finish before continuing the execution of the code

    //---------------------------------SPAWNING LOOP------------------------------
    for _ in 0..args.number_of_threads {
        // looping over the number of threads
        let counter = Arc::clone(&counter); // this is a a clone/reference that points to the same allocation of the counter variable
        let vec_storage = Arc::clone(&vec_storage); // clone of the vector storage
        let filename_thread = Arc::clone(&filename_thread); // clone of the filename


        let handle = thread::spawn(move || 
            // the thread takes ownership of all the variables that is within its scope
            {
            let mut counter_lock = counter.lock().unwrap(); //locking the shared variable counter before modifying it
            let start: usize = ((*counter_lock) * part_each) as usize; //the number of starting line in
            // println!("The thread number {} starts at {}",num, start);
            *counter_lock += 1; // incrementing the count of the created threads
            drop(counter_lock); //drops the lock over this variable as we do not need it anymore in this scope

            
            let (reader, _) = read_file(& *filename_thread); // data
            let mut current_row_iter:parquet::record::reader::RowIter = reader.get_row_iter(None).unwrap();
            for _ in 0..start-1 {
             current_row_iter.next();
                }

            let mut line: String = String::new(); // serves as a buffer;
            let mut vec: Vec<String> = Vec::new(); // the vector that stores the records of the current thread;
            for _ in 0..part_each {
                let current_row: Row= current_row_iter.next().unwrap(); 
                let columns_vec= current_row.get_column_iter().collect();
                vec.push(columns_vec);
                // vec.push(current_row); // we need to push a clone of it
                line.clear();
            }
                let mut vec_storage_lock = vec_storage.lock().unwrap(); //locking the shared vector
                vec_storage_lock.push(vec.clone());
                drop(vec_storage_lock); //drops the lock over this variable as we do not need it anymore in this scope
                vec.clear();
            }
        );
        handles.push(handle); //add the handle of the created thread to the vector
    }
    for handle in handles {
        handle.join().unwrap(); // waits for all the threads to finish
    }

     println!("The number of read record is: {}",(*vec_storage.lock().unwrap()).len()); // see if we successfully read all the records 
    
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