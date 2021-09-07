use std::fs::File;
use std::path::Path;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Row, RowAccessor};
use std::time::Instant;
use parquet::schema::types::Type;


fn main() {
    let now = Instant::now();    
    let path = "./SALES1.parquet";
    let file = File::open(&Path::new(path)).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let mut iter = reader.get_row_iter(None).unwrap();
    let mut data: Vec<Row> = Vec::new();
    while let Some(record) = iter.next() {
        // data.push(record);
    }
    let reading_time = now.elapsed();
    println!("Reading Csv {:.3?}", reading_time);    

    for f in reader.metadata().file_metadata().schema().get_fields() {
        println!("{:?}", f);
    }
    // for r in data {
    //     println!("{}", r);
    // }
}