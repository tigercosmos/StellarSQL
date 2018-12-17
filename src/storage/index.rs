use std::io;
use std::fs::File;
use std::io::SeekFrom;
use std::io::Seek;
use std::io::prelude::*;
use std::mem;

pub struct TableInfo {
    table_name: String,
    key_type: String,
    key_offet: u32,
    key_bytes: u32,
    row_bytes: u32,
}

struct IndexDataStructureInt {
    row: u32,
    key_value: u32,
}



fn type_int_construct(table_info: TableInfo) {
    let mut row = 0;
    let mut bytes_to_slide = table_info.row_bytes - table_info.key_bytes;
    let mut index_arr: Vec<IndexDataStructureInt> = Vec::new();
    let table_name = table_info.table_name.clone();
    let tableIndexName = table_name.clone() + "index";
    let mut file = File::open(table_name).unwrap();
    file.seek(SeekFrom::Start(table_info.key_offet as u64));
    let mut buffer = [0;4];
    loop {
        let bytes_read = match file.read(&mut buffer) {
            Ok(0) => break, // end-of-file
            Ok(n) => {
                unsafe {
                    let temp = mem::transmute::<[u8; 4], u32>(buffer);
                    let mut index_content = IndexDataStructureInt {
                        row: row,
                        key_value: temp,
                    };
                    index_arr.push(index_content);
                }
                file.seek(SeekFrom::Current(bytes_to_slide as i64));
                row = row + 1;
            },
            Err(e) => {
                println!("error{}", e);
            }
        };
    }

    index_arr.sort_unstable_by(|a, b| a.key_value.cmp(&b.key_value));
    let mut fileWrite = File::create(tableIndexName).unwrap();
    for i in 0..index_arr.len() {
        write!(fileWrite, "{:b}{:b}", index_arr[i].row, index_arr[i].key_value);
    }
}

fn type_string_construct(table_info: TableInfo) {
    //TODO....
}

pub fn construct_index(table_info: TableInfo) {
    if table_info.key_type == "Int" {
        type_int_construct(table_info);
    } else if table_info.key_type == "String" {
        type_string_construct(table_info);
    } else {
        println!("construct_index invalid");
    }
}
