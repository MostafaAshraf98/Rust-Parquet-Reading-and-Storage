use std::fs::File;
use std::path::Path;
use std::collections::HashMap;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Row, RowAccessor};
use std::time::Instant;
use std::env;
use parquet::schema::types::Type;
use parquet::basic::Type as PhysicalType;
use parquet::record::Field;

fn main() {
    let args: Vec<String> = env::args().collect();
    let (file_path, operation, column, group_by) = parse_args(&args);

    let now = Instant::now();    
    let (data, schema) = read_file(file_path);
    let reading_time = now.elapsed();
    println!("Reading Csv {:.3?}", reading_time);
    for f in schema.get_fields() {
        println!("{:?}", f);
    }
        // for r in data {
    //     println!("{}", r);
    // }

    let now = Instant::now();    
    let result = do_operation(&data, &schema, &operation, &column, &group_by);
    let reading_time = now.elapsed();
    println!("Operation {:.3?}", reading_time);

    match result {
        Ok(m) => {
            for (k, v) in m {
                println!("{} \t {:.3}", k, v);
            }
        }
        Err(()) => {
            println!("Error during Operation");
        }
    }
}

fn do_operation(data: &Vec<Row>, schema: &Type, operation: &str, column: &str, group_by: &str) -> Result<HashMap<String, f64>, ()> {
    let (column_idx, group_by_idx) = get_idx(schema, column, group_by);
    match operation.as_ref() {
        "avg" => Err(()),
        "sum" => sum(data, schema, column_idx, group_by_idx),
        _ => Err(())
    }
}

fn sum(data: &Vec<Row>, schema: &Type, column_idx: usize, group_by_idx: usize) -> Result<HashMap<String, f64>, ()> {
    // assuming field is integer
    let mut sum: HashMap<String, f64> = HashMap::new();

    for r in data {
        // println!("{:?}", r.get_column_iter().nth(group_by_idx));
        let key: String = r.get_column_iter().nth(group_by_idx).unwrap().1.to_string();
        let value: f64 = match r.get_column_iter().nth(column_idx).unwrap().1 {
            Field::Null => continue,
            _ => {
                r.get_column_iter().nth(column_idx).unwrap().1.to_string().parse::<f64>().unwrap()
            }
        };
        let entry = sum.entry(key).or_insert(0.0);
        *entry += value;
    }
    Ok(sum)
}

// match schema.get_fields()[group_by_idx].get_physical_type()  {
//     PhysicalType::BYTE_ARRAY => {
//         match schema.get_fields()[column_idx].get_physical_type() {
//             PhysicalType::INT32 => {},
//             PhysicalType::INT64 => {},
//             PhysicalType::INT96 => {},
//             PhysicalType::FLOAT => {},
//             PhysicalType::DOUBLE => {},
//             _ => {}
//         }
//     },
//     PhysicalType::INT32 => {},
//     PhysicalType::INT64 => {},
//     PhysicalType::INT96 => {},
//     PhysicalType::DOUBLE => {},
//     PhysicalType::FLOAT => {},
//     PhysicalType::BOOLEAN => {},
//     _ => {}
// }

fn get_idx(schema: &Type, column: &str, group_by: &str) -> (usize, usize) {
    let mut column_idx: usize = 0;
    let mut group_by_idx: usize = 0;
    
    // get idx of columns
    let mut i = 0;
    for f in schema.get_fields() {
        if f.name() == column {
            column_idx = i;
        } 
        if f.name() == group_by {
            group_by_idx = i;
        }
        i+=1;
    }
    (column_idx, group_by_idx)
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