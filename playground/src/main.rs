extern crate indexed_line_reader;

use indexed_line_reader::*;
use rayon::prelude::*;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

pub struct Args {
    pub filename: String, // the CSV file to read (the path)
    number_of_threads: u64,
    pub group_by: Option<String>, // list of columns to display
    pub query: Option<String>,    // query to filter the data
    pub select: Option<String>,   // column to apply aggreagation on
}

impl Args {
    pub fn new() -> Args {
        Args {
            filename: String::new(),
            number_of_threads: 0,
            group_by: None,
            query: None,
            select: None,
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
    args.query = env::args().nth(4);
    args.select = env::args().nth(5);
    let filename_thread = Arc::new(String::from(args.filename.clone())); //this serves as a shared ownership variable betweeen threads

    //---------------------------VARIABLES-----------------------------------

    let file = File::open(&args.filename).expect("Unable to open file");
    let reader = BufReader::new(file);

    let count: u64 = reader.lines().fold(0, |sum, _| sum + 1); // the number of records in the file
    println!("The number of records in the file is: {}", count);
    let part_each: u64 = (count as f64 / args.number_of_threads as f64).ceil() as u64; //the number of records each thread will take
    println!("part_each = {}",part_each);
    let vec_storage = Arc::new(Mutex::new(vec![vec![]])); // stores the records after reading them

    let counter = Arc::new(Mutex::new(0)); // this keeps count of the threads we created so far and it is shared between the threads
    let mut handles = vec![]; // this is the vector of handles of all the created threads so that the main waits for them to finish before continuing the execution of the code

    //---------------------------------SPAWNING LOOP------------------------------
    for _ in 0..args.number_of_threads {
        // looping over the number of threads
        let counter = Arc::clone(&counter); // this is a a clone/reference that points to the same allocation of the counter variable
        let vec_storage_clone = Arc::clone(&vec_storage); // clone of the vector storage
        let filename_clone = Arc::clone(&filename_thread); // clone of the filename

        let handle = thread::spawn(move || 
            // the thread take ownership of all the variables that is within its scope
            {
            let mut num = counter.lock().unwrap(); //locking the shared variable counter before modifying it
            let start: u64 = ((*num) * part_each) as u64; //the number of starting line in
            // println!("The thread number {} starts at {}",num, start);
            *num += 1; // incrementing the count of the created threads
            std::mem::drop(num); //drops the lock over this variable as we do not need it anymore in this scope

            let mut vec_storage_result = vec_storage_clone.lock().unwrap(); //locking the shared vector

            let f = File::open(&*filename_clone).expect("Unable to open file");
            let reader = &mut IndexedLineReader::new(BufReader::new(f), part_each);
            reader.seek(SeekFrom::Start(start)).expect("Unable to seek over the required line"); // move the reader to the required line
            let mut line: String = String::new(); // serves as a buffer;
            
            for _ in 0..part_each {
                let eof = reader.read_line(&mut line).expect("Unable to read line"); // read the line
                if eof != 0 
                // if it is not the end of file... this is used as a safe net 
                {
                    // vec_storage_result.push(vec!["hi"]);
                    vec_storage_result.push(line.clone().par_split(',').collect()); // we need to push a clone of it
                    line.clear();
                }
            }
        });
        handles.push(handle); //add the handle of the created thread to the vector
    }
    for handle in handles {
        handle.join().unwrap(); // waits for all the threads to finish
    }

    let vec_storage_clone = vec_storage.lock().unwrap();
    println!("The number of read record is: {}",(*vec_storage_clone).len()); // see if we successfully read all the records 
    
    let end_time = start_time.elapsed(); // calculates the elapsed time
    println!("The reading time is: {:?}", end_time);
}
