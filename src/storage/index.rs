use std::fmt;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::Seek;
use std::io::SeekFrom;
use std::mem;

#[derive(Debug)]
pub enum IndexErr {
    BuildIntIndexTableError,
    ReadIntIndexTableError,
    BuildStringIndexTableError,
    ReadStringIndexTableError,
}

impl fmt::Display for IndexErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            IndexErr::BuildIntIndexTableError => write!(f, "Build int index table error"),
            IndexErr::ReadIntIndexTableError => write!(f, "Read int index table error"),
            IndexErr::BuildStringIndexTableError => write!(f, "Build string index table error"),
            IndexErr::ReadStringIndexTableError => write!(f, "Read string index table error"),
        }
    }
}

// meta data of raw table
pub struct TableMeta {
    table_name: String, // name of raw table
    key_type: String,   // type of primary key in raw table
    key_offet: u32,     // byte position of first primary key in raw table
    key_bytes: u32,     // bytes of primary key in raw table
    row_bytes: u32,     // bytes of each row in raw table
}

// row and key value pair in which key type is int
pub struct IndexDataStructureInt {
    row: u32,
    key_value: u32,
}
// row and key value pair in which key type is string
pub struct IndexDataStructureString {
    row: u32,
    key_value: Vec<u8>,
}

pub struct IndexInt(Vec<IndexDataStructureInt>);
pub struct IndexString(Vec<IndexDataStructureString>);


// build index table with raw table in whicj key type is int
pub fn build_int_index_table(table_meta: &TableMeta, index_arr: &mut IndexInt) -> Result<(), IndexErr> {
    let mut row = 0;
    let mut bytes_to_slide = table_meta.row_bytes - table_meta.key_bytes;
    let table_name = table_meta.table_name.clone();
    let mut file = File::open(table_name).unwrap();
    file.seek(SeekFrom::Start(table_meta.key_offet as u64));
    let mut buffer = [0; 4];
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
                    index_arr.0.push(index_content);
                }
                file.seek(SeekFrom::Current(bytes_to_slide as i64));
                row = row + 1;
            }
            Err(e) => {
                return Err(IndexErr::BuildIntIndexTableError);
            }
        };
    }

    index_arr.0.sort_unstable_by(|a, b| a.key_value.cmp(&b.key_value));
    Ok(())
}

// write index table into index file
pub fn write_int_index_table(table_meta: &TableMeta, index_arr: &mut IndexInt) -> Result<(), IndexErr> {
    let table_index_name = table_meta.table_name.clone() + "index";
    let mut file_write = File::create(table_index_name).unwrap();
    for i in 0..index_arr.0.len() {
        let row_temp = unsafe { mem::transmute::<u32, [u8; 4]>(index_arr.0[i].row) };
        file_write.write(&row_temp);
        let key_temp = unsafe { mem::transmute::<u32, [u8; 4]>(index_arr.0[i].key_value) };
        file_write.write(&key_temp);
    }
    Ok(())
}

// read index table from index file
pub fn read_int_index_table(table_meta: &TableMeta, index_arr: &mut IndexInt) -> Result<(), IndexErr> {
    let table_index_name = table_meta.table_name.clone() + "index";
    let mut file = File::open(table_index_name).unwrap();
    let mut buffer_row = [0; 4];
    let mut buffer_key = [0; 4];
    loop {
        let bytes_read = match file.read(&mut buffer_row) {
            Ok(0) => break, // end-of-file
            Ok(n) => unsafe {
                let temp_row = mem::transmute::<[u8; 4], u32>(buffer_row);
                file.read(&mut buffer_key);
                let temp_key = mem::transmute::<[u8; 4], u32>(buffer_key);
                let mut index_content = IndexDataStructureInt {
                    row: temp_row,
                    key_value: temp_key,
                };
                index_arr.0.push(index_content);
            },
            Err(e) => {
                return Err(IndexErr::ReadIntIndexTableError);
            }
        };
    }
    Ok(())
}

// insert into index table in which key type is int
// if work, use b-insert
pub fn insert_int_index_table(insert_value: IndexDataStructureInt, index_arr: &mut IndexInt) -> Result<(), IndexErr> {
    if (index_arr.0.is_empty()) {
        index_arr.0.push(insert_value);
    } else {
        let mut target = 0;
        for i in 0..index_arr.0.len() {
            if (insert_value.key_value <= index_arr.0[i].key_value) {
                target = i;
                break;
            }
        }
        if (target == 0) {
            index_arr.0.insert(target, insert_value);
        } else {
            index_arr.0.insert(target - 1, insert_value);
        }
    }
    Ok(())
}

pub fn build_string_index_table(table_meta: &TableMeta, index_arr: &mut IndexString) -> Result<(), IndexErr> {
    let mut row = 0;
    let mut bytes_to_slide = table_meta.row_bytes - table_meta.key_bytes;
    let table_name = table_meta.table_name.clone();
    let mut file = File::open(table_name).unwrap();
    file.seek(SeekFrom::Start(table_meta.key_offet as u64));
    let mut buffer = vec![0; table_meta.key_bytes as usize];
    loop {
        let bytes_read = match file.read(&mut buffer) {
            Ok(0) => break, // end-of-file
            Ok(n) => {
                let mut index_content = IndexDataStructureString {
                    row: row,
                    key_value: buffer.clone(),
                };
                index_arr.0.push(index_content);
                file.seek(SeekFrom::Current(bytes_to_slide as i64));
                row = row + 1;
            }
            Err(e) => {
                return Err(IndexErr::BuildStringIndexTableError);
            }
        };
    }

    index_arr.0.sort_unstable_by(|a, b| a.key_value.cmp(&b.key_value));
    Ok(())
}

pub fn write_string_index_table(table_meta: &TableMeta, index_arr: &mut IndexString) -> Result<(), IndexErr> {
    let table_index_name = table_meta.table_name.clone() + "index";
    let mut file_write = File::create(table_index_name).unwrap();
    for i in 0..index_arr.0.len() {
        let row_temp = unsafe { mem::transmute::<u32, [u8; 4]>(index_arr.0[i].row) };
        file_write.write(&row_temp);
        file_write.write(&index_arr.0[i].key_value);
    }
    Ok(())
}

pub fn read_string_index_table(table_meta: &TableMeta, index_arr: &mut IndexString) -> Result<(), IndexErr> {
    let table_index_name = table_meta.table_name.clone() + "index";
    let mut file = File::open(table_index_name).unwrap();
    let mut buffer_row = [0; 4];
    let mut buffer_key = vec![0; table_meta.key_bytes as usize];
    loop {
        let bytes_read = match file.read(&mut buffer_row) {
            Ok(0) => break, // end-of-file
            Ok(n) => unsafe {
                let temp_row = mem::transmute::<[u8; 4], u32>(buffer_row);
                file.read(&mut buffer_key);
                let mut index_content = IndexDataStructureString {
                    row: temp_row,
                    key_value: buffer_key.clone(),
                };
                index_arr.0.push(index_content);
            },
            Err(e) => {
                return Err(IndexErr::ReadStringIndexTableError);
            }
        };
    }
    Ok(())
}

pub fn insert_string_index_table(
    insert_value: IndexDataStructureString,
    index_arr: &mut IndexString,
) -> Result<(), IndexErr> {
    if (index_arr.0.is_empty()) {
        index_arr.0.push(insert_value);
    } else {
        let mut target = 0;
        for i in 0..index_arr.0.len() {
            if (insert_value.key_value <= index_arr.0[i].key_value) {
                target = i;
                break;
            }
        }
        if (target == 0) {
            index_arr.0.insert(target, insert_value);
        } else {
            index_arr.0.insert(target - 1, insert_value);
        }
    }
    Ok(())
}

#[test]
pub fn test_construct_index() {
    let table_meta = TableMeta {
        table_name: String::from("1.in"),
        key_type: String::from("Int"),
        key_offet: 0,
        key_bytes: 4,
        row_bytes: 4,
    };
    let mut index_arr = IndexInt(vec![]);
    build_int_index_table(&table_meta, &mut index_arr);
    write_int_index_table(&table_meta, &mut index_arr);
}

#[test]
pub fn test_read_index() {
    let table_meta = TableMeta {
        table_name: String::from("1.in"),
        key_type: String::from("Int"),
        key_offet: 0,
        key_bytes: 4,
        row_bytes: 4,
    };
    let mut index_arr = IndexInt(vec![]);
    read_int_index_table(&table_meta, &mut index_arr);
    write_int_index_table(&table_meta, &mut index_arr);
}
