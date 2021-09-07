use std::fs::File;
use std::path::Path;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::time::Instant;
use parquet::column::reader::ColumnReader;
// use parquet::schema::types::Type;
use parquet::column::reader::ColumnReaderImpl;
use parquet::data_type::{Int64Type};

fn main() {
    // let now = Instant::now();    
    let path = "./SALES1.parquet";
    let file = File::open(&Path::new(path)).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let fields = reader.metadata().file_metadata().schema().get_fields();
    for i in 0..reader.num_row_groups() {
        println!("row group {}", i);
        let row_group = reader.get_row_group(i).unwrap();
        let num_rows = row_group.metadata().num_rows();
        println!("{}", num_rows);
        for j in 0..row_group.num_columns() {
            println!("row group {} column {}: {:?}", i, j, fields[j].name());
            match row_group.get_column_reader(j).unwrap() {
                ColumnReader::BoolColumnReader(_) => { println!("Bool"); },
                ColumnReader::ByteArrayColumnReader(_) => { println!("Byte"); },
                ColumnReader::DoubleColumnReader(_) => { println!("Double"); },
                ColumnReader::FixedLenByteArrayColumnReader(_) => { println!("FixedLen"); },
                ColumnReader::FloatColumnReader(_) => { println!("FLoat"); },
                ColumnReader::Int32ColumnReader(_) => { println!("Int32"); },
                ColumnReader::Int64ColumnReader(v) => { 
                    println!("Int64");
                    let now = Instant::now();
                    let (column, count) = read_i64(v, num_rows as usize); 
                    let read_time = now.elapsed();
                    println!("read {} value, Reading time = {:?}", count, read_time);
                    let now = Instant::now();
                    let s = sum(&column, count);
                    let sum_time = now.elapsed();
                    println!("Sum = {}, Sum time = {:?}", s, sum_time);
                },             
                ColumnReader::Int96ColumnReader(_) => { 
                    println!("Int96"); 
                },
            }
        }
    }
}

fn sum(data: &[i64], count: usize) -> i64 {
    let mut sum: i64 = 0;
    for i in 0..count {
        sum += data[i];
    }
    sum
}

fn read_i64(mut column_reader: ColumnReaderImpl<Int64Type>, num_rows: usize) -> (Vec<i64>, usize) {
    let mut data = vec![];
    const BATCH_SIZE: usize = 100;
    let mut count = 0;
    for _ in 0..(num_rows as f64 / BATCH_SIZE as f64).ceil() as i64 {
        let mut values: [i64; BATCH_SIZE] = [0; BATCH_SIZE];
        let (num, _) = column_reader.read_batch(BATCH_SIZE, None, None, &mut values).unwrap();
        count += num;
        data.extend(values);
    }
    (data, count)
}