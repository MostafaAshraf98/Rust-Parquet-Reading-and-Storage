use std::fs::File;
use std::path::Path;
use std::collections::HashMap;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Row, RowAccessor};
use std::time::Instant;
use std::env;
use parquet::schema::types::Type;

fn main() {
    let args: Vec<String> = env::args().collect();
    let (file_path, operation, column, group_by) = parse_args(&args);

    let now = Instant::now();    
    let (data, schema) = read_file(file_path);
    let reading_time = now.elapsed();
    println!("Reading Csv {:.3?}", reading_time);

        // for r in data {
    //     println!("{}", r);
    // }

    let now = Instant::now();    
    let result = do_operation(&data, &schema, &operation, &column, &group_by);
    let reading_time = now.elapsed();
    println!("Operation {:.3?}", reading_time);

}

fn do_operation(data: &Vec<Row>, schema: &Type, operation: &str, column: &str, group_by: &str) -> Result<HashMap<String, f64>, ()> {
    match operation.as_ref() {
        "avg" => {},
        "sum" => {},
        _ => {}
    }
    Err(())
}

fn sum(data: &Vec<Row>, schema: &Type, column: &str, group_by: &str) -> Result<HashMap<String, f64>, ()> {
    // assuming field is integer
    let mut sum: HashMap<String, f64> = HashMap::new();
    
    let mut column_idx = 0;
    let mut group_by_idx = 0;
    
    // get idx of columns
    let mut i = 0;
    for f in schema.get_fields() {
        if f.name() == column {
            column_idx = i;
        } 
        if f.name() == group_by {
            group_by_idx = 1;
        }
        i+=1;
    }

    for r in data {
        let entry = sum.entry(r.get_string(group_by_idx).unwrap().to_string()).or_insert(0.0);
        *entry += r.get_int(column_idx).unwrap() as f64;
    }
    Ok(sum)
}


fn read_file(file_path: &str) -> (Vec<Row>, Type) {
    let file = File::open(&Path::new(file_path)).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let mut iter = reader.get_row_iter(None).unwrap();
    let mut data: Vec<Row> = Vec::new();
    while let Some(record) = iter.next() {
        data.push(record);
    }
    let schema = reader.metadata().file_metadata().schema().clone();
    (data, schema)
}

fn parse_args(args: &[String]) -> (&str, &str, &str, &str) {
    if env::args().len() != 5 {
        panic!("args error");
    }
    let file_path = &args[1];
    let operation = &args[2];
    let column = &args[3];
    let group_by = &args[4];

    (file_path, operation, column, group_by)
}