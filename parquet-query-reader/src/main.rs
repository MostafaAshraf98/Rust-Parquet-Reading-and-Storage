// use parquet::basic::Type as PhysicalType;
use parquet::file::reader::{FileReader, SerializedFileReader};
// use parquet::record::Field;
use parquet::record::{Row, RowAccessor};
use parquet::schema::types::Type;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::path::Path;
use std::time::Instant;

fn main() {
    let args: Vec<String> = env::args().collect();
    let (file_path, operation, column, group_by) = parse_args(&args);

    let now = Instant::now();
    let (data, schema) = read_file(file_path);
    let reading_time = now.elapsed();
    println!("Reading File {:.3?}", reading_time);

    // for f in schema.get_fields() {
    //     println!("{:?}", f);
    // }
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

fn do_operation(
    data: &SerializedFileReader<File>,
    schema: &Type,
    operation: &str,
    column: &str,
    group_by: &str,
) -> Result<HashMap<i64, f64>, ()> {
    // let (column_idx, group_by_idx) = get_idx(schema, column, group_by);
    match operation.as_ref() {
        "avg" => Err(()),
        "sum" => sum(data, schema, column, group_by),
        _ => Err(()),
    }
}

fn sum(
    reader: &SerializedFileReader<File>,
    schema: &Type,
    column: &str,
    group_by: &str,
) -> Result<HashMap<i64, f64>, ()> {
    let mut sum: HashMap<i64, f64> = HashMap::new();

    //----------------- create schema projection
    let requested_fields = vec![column, group_by];
    let mut selected_fields = schema.get_fields().to_vec();
    if requested_fields.len() > 0 {
        selected_fields.retain(|f| requested_fields.contains(&f.name()));
    }

    // Now build a schema from these selected fields:
    let schema_projection = Type::group_type_builder("schema")
        .with_fields(&mut selected_fields)
        .build()
        .unwrap();

    let data: Vec<Row> = reader
        .get_row_iter(Some(schema_projection))
        .unwrap()
        .collect();
    let mut column_idx = 0;
    let mut group_by_idx = 1;
    if data[0].get_column_iter().nth(0).unwrap().0 == group_by {
        column_idx = 1;
        group_by_idx = 0;
    }

    //------------using row accessor
    for r in data {
        let key: i64 = r.get_long(group_by_idx).unwrap();
        let value: f64 = r.get_double(column_idx).unwrap();
        let entry = sum.entry(key).or_insert(0.0);
        *entry += value;
    }

    //------------------using column iter
    // for r in data {
    //     let key: String = r.get_column_iter().nth(group_by_idx).unwrap().1.to_string();
    //     let value: f64 = match r.get_column_iter().nth(column_idx).unwrap().1 {
    //         Field::Null => continue,
    //         _ => {
    //             r.get_column_iter().nth(column_idx).unwrap().1.to_string().parse::<f64>().unwrap()
    //         }
    //     };
    //     let entry = sum.entry(key).or_insert(0.0);
    //     *entry += value;
    // }

    // -----------------------------using iter
    // while let Some(r) = iter.next() {
    //     // println!("{:?}", r.get_column_iter().nth(group_by_idx));
    //     let key: String = r.get_column_iter().nth(group_by_idx).unwrap().1.to_string();
    //     let value: f64 = match r.get_column_iter().nth(column_idx).unwrap().1 {
    //         Field::Null => continue,
    //         _ => {
    //             r.get_column_iter().nth(column_idx).unwrap().1.to_string().parse::<f64>().unwrap()
    //         }
    //     };
    //     let entry = sum.entry(key).or_insert(0.0);
    //     *entry += value;
    // }

    //---------------------------- using Vec<Row>----------------------
    // for r in data {
    //     // println!("{:?}", r.get_column_iter().nth(group_by_idx));
    //     let key: String = r.get_column_iter().nth(group_by_idx).unwrap().1.to_string();
    //     let value: f64 = match r.get_column_iter().nth(column_idx).unwrap().1 {
    //         Field::Null => continue,
    //         _ => {
    //             r.get_column_iter().nth(column_idx).unwrap().1.to_string().parse::<f64>().unwrap()
    //         }
    //     };
    //     let entry = sum.entry(key).or_insert(0.0);
    //     *entry += value;
    // }
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

// fn get_idx(schema: &Type, column: &str, group_by: &str) -> (usize, usize) {
//     let mut column_idx: usize = 0;
//     let mut group_by_idx: usize = 0;
//     // get idx of columns
//     let mut i = 0;
//     for f in schema.get_fields() {
//         if f.name() == column {
//             column_idx = i;
//         }
//         if f.name() == group_by {
//             group_by_idx = i;
//         }
//         i += 1;
//     }
//     (column_idx, group_by_idx)
// }

fn read_file(file_path: &str) -> (SerializedFileReader<File>, Type) {
    let file = File::open(&Path::new(file_path)).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    // let mut iter = reader.get_row_iter(None).unwrap();
    // let mut data: Vec<Row> = Vec::new();
    // while let Some(record) = iter.next() {
    //     data.push(record);
    // }
    let schema = reader.metadata().file_metadata().schema().clone();
    // (data, schema)
    (reader, schema)
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
