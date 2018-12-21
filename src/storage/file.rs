use crate::component::datatype::DataType;
use crate::component::field;
use crate::component::field::Field;
use crate::component::table::Table;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct File {
    /* definition */
// Ideally, File is a stateless struct
}

#[derive(Debug)]
pub enum FileError {
    Io,
    BaseDirNotExists,
    UsernamesJsonNotExists,
    UsernameExists,
    UsernameNotExists,
    UsernameDirNotExists,
    DbsJsonNotExists,
    DbExists,
    DbNotExists,
    DbDirNotExists,
    TablesJsonNotExists,
    TableExists,
    TableNotExists,
    TableTsvNotExists,
    JsonParse,
}

#[derive(Debug, Serialize, Deserialize)]
struct UsernamesJson {
    usernames: Vec<UsernameInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
struct UsernameInfo {
    name: String,
    path: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct DbsJson {
    dbs: Vec<DbInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
struct DbInfo {
    name: String,
    path: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct TablesJson {
    tables: Vec<TableInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableInfo {
    name: String,
    path_tsv: String,
    path_bin: String,
    primary_key: Vec<String>,
    foreign_key: Vec<String>,
    reference_table: Option<String>,
    // reference_attr: Option<String>,
    attrs_order: Vec<String>,
    attrs: HashMap<String, Field>,
    last_rid: u32,
}

impl From<io::Error> for FileError {
    fn from(_err: io::Error) -> FileError {
        FileError::Io
    }
}

impl From<serde_json::Error> for FileError {
    fn from(_err: serde_json::Error) -> FileError {
        FileError::JsonParse
    }
}

impl fmt::Display for FileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            FileError::Io => write!(f, "No such file or directory."),
            FileError::BaseDirNotExists => write!(f, "Base data directory not exists. All data lost."),
            FileError::UsernamesJsonNotExists => write!(f, "The file `usernames.json` is lost"),
            FileError::UsernameExists => write!(f, "User name already exists and cannot be created again."),
            FileError::UsernameNotExists => {
                write!(f, "Specified user name not exists. Please create this username first.")
            }
            FileError::UsernameDirNotExists => write!(f, "Username exists but corresponding data folder is lost."),
            FileError::DbsJsonNotExists => write!(f, "The `dbs.json` of the username is lost"),
            FileError::DbExists => write!(f, "DB already exists and cannot be created again."),
            FileError::DbNotExists => write!(f, "DB not exists. Please create DB first."),
            FileError::DbDirNotExists => write!(f, "DB exists but correspoding data folder is lost."),
            FileError::TablesJsonNotExists => write!(f, "The `tables.json` of the DB is lost."),
            FileError::TableExists => write!(f, "Table already exists and cannot be created again."),
            FileError::TableNotExists => write!(f, "Table not exists. Please create table first."),
            FileError::TableTsvNotExists => write!(f, "Table exists but correspoding tsv file is lost."),
            FileError::JsonParse => write!(f, "JSON parsing error."),
        }
    }
}

impl File {
    pub fn create_username(username: &str, file_base_path: Option<&str>) -> Result<(), FileError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // create base data folder if not exists
        if !Path::new(base_path).exists() {
            fs::create_dir_all(base_path)?;
        }

        // load current usernames from `usernames.json`
        let mut usernames_json: UsernamesJson;
        let usernames_json_path = format!("{}/{}", base_path, "usernames.json");
        if Path::new(&usernames_json_path).exists() {
            let usernames_file = fs::File::open(&usernames_json_path)?;
            usernames_json = serde_json::from_reader(usernames_file)?;
        } else {
            usernames_json = UsernamesJson { usernames: Vec::new() };
        }

        // check if the username exists
        for username_info in &usernames_json.usernames {
            if username_info.name == username {
                return Err(FileError::UsernameExists);
            }
        }

        // create new username json instance
        let new_username_info = UsernameInfo {
            name: username.to_string(),
            path: username.to_string(),
        };

        // insert the new username record into `usernames.json`
        usernames_json.usernames.push(new_username_info);

        // save `usernames.json`
        let mut usernames_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(usernames_json_path)?;
        usernames_file.write_all(serde_json::to_string_pretty(&usernames_json)?.as_bytes())?;

        // create corresponding directory for the new username
        let username_path = format!("{}/{}", base_path, username);
        fs::create_dir_all(&username_path)?;

        // create corresponding `dbs.json` for the new username
        let dbs_json_path = format!("{}/{}", username_path, "dbs.json");
        let mut dbs_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dbs_json_path)?;
        let dbs_json = DbsJson { dbs: Vec::new() };
        dbs_file.write_all(serde_json::to_string_pretty(&dbs_json)?.as_bytes())?;

        Ok(())
    }

    pub fn get_usernames(file_base_path: Option<&str>) -> Result<Vec<String>, FileError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward base level
        match File::storage_hierarchy_check(base_path, None, None, None) {
            Ok(_) => (),
            Err(e) => return Err(e),
        };

        // read and parse `usernames.json`
        let usernames_json_path = format!("{}/{}", base_path, "usernames.json");
        let usernames_file = fs::File::open(&usernames_json_path)?;
        let usernames_json: UsernamesJson = serde_json::from_reader(usernames_file)?;

        // create a vector of usernames
        let usernames = usernames_json
            .usernames
            .iter()
            .map(|username_info| username_info.name.clone())
            .collect::<Vec<String>>();
        Ok(usernames)
    }

    pub fn remove_username(username: &str, file_base_path: Option<&str>) -> Result<(), FileError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward base level
        match File::storage_hierarchy_check(base_path, None, None, None) {
            Ok(_) => (),
            Err(e) => return Err(e),
        };

        // read and parse `usernames.json`
        let usernames_json_path = format!("{}/{}", base_path, "usernames.json");
        let usernames_file = fs::File::open(&usernames_json_path)?;
        let mut usernames_json: UsernamesJson = serde_json::from_reader(usernames_file)?;

        // remove if the username exists; otherwise raise error
        let idx_to_remove = usernames_json
            .usernames
            .iter()
            .position(|username_info| &username_info.name == username);
        match idx_to_remove {
            Some(idx) => usernames_json.usernames.remove(idx),
            None => return Err(FileError::UsernameNotExists),
        };

        // remove corresponding username directory
        let username_path = format!("{}/{}", base_path, username);
        if Path::new(&username_path).exists() {
            fs::remove_dir_all(&username_path)?;
        }

        // overwrite `usernames.json`
        let mut usernames_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(usernames_json_path)?;
        usernames_file.write_all(serde_json::to_string_pretty(&usernames_json)?.as_bytes())?;

        Ok(())
    }

    pub fn create_db(username: &str, db_name: &str, file_base_path: Option<&str>) -> Result<(), FileError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward username level
        match File::storage_hierarchy_check(base_path, Some(username), None, None) {
            Ok(_) => (),
            Err(e) => return Err(e),
        };

        // load current dbs from `dbs.json`
        let dbs_json_path = format!("{}/{}/{}", base_path, username, "dbs.json");
        let dbs_file = fs::File::open(&dbs_json_path)?;
        let mut dbs_json: DbsJson = serde_json::from_reader(dbs_file)?;

        // check if the db exists
        for db_info in &dbs_json.dbs {
            if db_info.name == db_name {
                return Err(FileError::DbExists);
            }
        }

        // create new db json instance
        let new_db_info = DbInfo {
            name: db_name.to_string(),
            path: db_name.to_string(),
        };

        // insert the new db record into `dbs.json`
        dbs_json.dbs.push(new_db_info);

        // save `dbs.json`
        let mut dbs_file = fs::OpenOptions::new().write(true).truncate(true).open(dbs_json_path)?;
        dbs_file.write_all(serde_json::to_string_pretty(&dbs_json)?.as_bytes())?;

        // create corresponding directory for the db
        let db_path = format!("{}/{}/{}", base_path, username, db_name);
        fs::create_dir_all(&db_path)?;

        // create corresponding `tables.json` for the new db
        let tables_json_path = format!("{}/{}", db_path, "tables.json");
        let mut tables_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(tables_json_path)?;
        let tables_json = TablesJson { tables: Vec::new() };
        tables_file.write_all(serde_json::to_string_pretty(&tables_json)?.as_bytes())?;

        Ok(())
    }

    pub fn get_dbs(username: &str, file_base_path: Option<&str>) -> Result<Vec<String>, FileError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward username level
        match File::storage_hierarchy_check(base_path, Some(username), None, None) {
            Ok(_) => (),
            Err(e) => return Err(e),
        };

        // read and parse `dbs.json`
        let dbs_json_path = format!("{}/{}/{}", base_path, username, "dbs.json");
        let dbs_file = fs::File::open(&dbs_json_path)?;
        let dbs_json: DbsJson = serde_json::from_reader(dbs_file)?;

        // create a vector of dbs
        let dbs = dbs_json
            .dbs
            .iter()
            .map(|db_info| db_info.name.clone())
            .collect::<Vec<String>>();
        Ok(dbs)
    }

    pub fn remove_db(username: &str, db_name: &str, file_base_path: Option<&str>) -> Result<(), FileError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward username level
        match File::storage_hierarchy_check(base_path, Some(username), None, None) {
            Ok(_) => (),
            Err(e) => return Err(e),
        };

        // load current dbs from `dbs.json`
        let dbs_json_path = format!("{}/{}/{}", base_path, username, "dbs.json");
        let dbs_file = fs::File::open(&dbs_json_path)?;
        let mut dbs_json: DbsJson = serde_json::from_reader(dbs_file)?;

        // remove if the db exists; otherwise raise error
        let idx_to_remove = dbs_json.dbs.iter().position(|db_info| &db_info.name == db_name);
        match idx_to_remove {
            Some(idx) => dbs_json.dbs.remove(idx),
            None => return Err(FileError::DbNotExists),
        };

        // remove corresponding db directory
        let db_path = format!("{}/{}/{}", base_path, username, db_name);
        if Path::new(&db_path).exists() {
            fs::remove_dir_all(&db_path)?;
        }

        // overwrite `dbs.json`
        let mut dbs_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(dbs_json_path)?;
        dbs_file.write_all(serde_json::to_string_pretty(&dbs_json)?.as_bytes())?;

        Ok(())
    }

    pub fn create_table(
        username: &str,
        db_name: &str,
        table: &Table,
        file_base_path: Option<&str>,
    ) -> Result<(), FileError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward db level
        match File::storage_hierarchy_check(base_path, Some(username), Some(db_name), None) {
            Ok(_) => (),
            Err(e) => return Err(e),
        };

        // load current tables from `tables.json`
        let tables_json_path = format!("{}/{}/{}/{}", base_path, username, db_name, "tables.json");
        let tables_file = fs::File::open(&tables_json_path)?;
        let mut tables_json: TablesJson = serde_json::from_reader(tables_file)?;

        // check if the table exists
        for table_info in &tables_json.tables {
            if table_info.name == table.name {
                return Err(FileError::TableExists);
            }
        }

        // create new table json instance
        let mut new_table_info = TableInfo {
            name: table.name.to_string(),
            path_tsv: format!("{}.tsv", table.name),
            path_bin: format!("{}.bin", table.name),
            primary_key: table.primary_key.clone(),
            foreign_key: table.foreign_key.clone(),
            reference_table: table.reference_table.clone(),
            // reference_attr: table.reference_attr.clone(),
            attrs_order: vec![],
            attrs: table.fields.clone(),
            last_rid: 0,
        };

        // determine storing order of attrs in .tsv and .bin
        // `__rid__` and primary key attrs are always at first
        new_table_info.attrs_order = vec!["__rid__".to_string()];
        new_table_info
            .attrs_order
            .extend_from_slice(&new_table_info.primary_key);
        for (k, _v) in table.fields.iter() {
            if !new_table_info.primary_key.contains(&k) {
                new_table_info.attrs_order.push(k.clone());
            }
        }

        // create corresponding tsv for the table, with the title line
        let table_tsv_path = format!("{}/{}/{}/{}", base_path, username, db_name, new_table_info.path_tsv);
        let mut table_tsv_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(table_tsv_path)?;
        table_tsv_file.write_all(new_table_info.attrs_order.join("\t").as_bytes())?;

        // insert the new table record into `tables.json`
        tables_json.tables.push(new_table_info);

        // save `tables.json`
        let mut tables_file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(tables_json_path)?;
        tables_file.write_all(serde_json::to_string_pretty(&tables_json)?.as_bytes())?;

        Ok(())
    }

    pub fn load_tables(
        username: &str,
        db_name: &str,
        file_base_path: Option<&str>,
    ) -> Result<Vec<TableInfo>, FileError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward db level
        match File::storage_hierarchy_check(base_path, Some(username), Some(db_name), None) {
            Ok(_) => (),
            Err(e) => return Err(e),
        };

        // load current tables from `tables.json`
        let tables_json_path = format!("{}/{}/{}/{}", base_path, username, db_name, "tables.json");
        let tables_file = fs::File::open(&tables_json_path)?;
        let tables_json: TablesJson = serde_json::from_reader(tables_file)?;

        // return the vector of table info
        Ok(tables_json.tables)
    }

    pub fn drop_table(
        username: &str,
        db_name: &str,
        table_name: &str,
        file_base_path: Option<&str>,
    ) -> Result<(), FileError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward table level
        match File::storage_hierarchy_check(base_path, Some(username), Some(db_name), Some(table_name)) {
            Ok(_) => (),
            Err(e) => return Err(e),
        };

        // load current tables from `tables.json`
        let tables_json_path = format!("{}/{}/{}/{}", base_path, username, db_name, "tables.json");
        let tables_file = fs::File::open(&tables_json_path)?;
        let mut tables_json: TablesJson = serde_json::from_reader(tables_file)?;

        // remove if the table exists; otherwise raise error
        let idx_to_remove = tables_json
            .tables
            .iter()
            .position(|table_info| &table_info.name == table_name);
        match idx_to_remove {
            Some(idx) => tables_json.tables.remove(idx),
            None => return Err(FileError::TableNotExists),
        };

        // remove corresponding tsv file
        let table_tsv_path = format!("{}/{}/{}/{}.tsv", base_path, username, db_name, table_name);
        if Path::new(&table_tsv_path).exists() {
            fs::remove_file(&table_tsv_path)?;
        }

        // overwrite `tables.json`
        let mut tables_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(tables_json_path)?;
        tables_file.write_all(serde_json::to_string_pretty(&tables_json)?.as_bytes())?;

        Ok(())
    }

    // TODO: append_rows(username: &str, db_name: &str, table_name: &str, rows: &Vec<Row>, file_base_path: Option<&str>) -> Result<Vec<u32>, FileError>

    // TODO: fetch_rows(username: &str, db_name: &str, table_name: &str, row_id_range: &Vec<u32>, file_base_path: Option<&str>) -> Result<Vec<Row>, FileError>

    // TODO: delete_rows(username: &str, db_name: &str, table_name: &str, row_id_range: &Vec<u32>, file_base_path: Option<&str>) -> Result<(), FileError>

    // TODO: modify_row(username: &str, db_name: &str, table_name: &str, row_id: u32, new_row: &Row, file_base_path: Option<&str>) -> Result<(), FileError>

    fn storage_hierarchy_check(
        base_path: &str,
        username: Option<&str>,
        db_name: Option<&str>,
        table_name: Option<&str>,
    ) -> Result<(), FileError> {
        // check if base directory exists
        if !Path::new(base_path).exists() {
            return Err(FileError::BaseDirNotExists);
        }

        // check if `usernames.json` exists
        let usernames_json_path = format!("{}/{}", base_path, "usernames.json");
        if !Path::new(&usernames_json_path).exists() {
            return Err(FileError::UsernamesJsonNotExists);
        }

        // base level check passed
        if username == None {
            return Ok(());
        }

        // check if username exists
        let usernames_file = fs::File::open(&usernames_json_path)?;
        let usernames_json: UsernamesJson = serde_json::from_reader(usernames_file)?;
        if !usernames_json
            .usernames
            .iter()
            .map(|username_info| username_info.name.clone())
            .collect::<Vec<String>>()
            .contains(&username.unwrap().to_string())
        {
            return Err(FileError::UsernameNotExists);
        }

        // check if username directory exists
        let username_path = format!("{}/{}", base_path, username.unwrap());
        if !Path::new(&username_path).exists() {
            return Err(FileError::UsernameDirNotExists);
        }

        // check if `dbs.json` exists
        let dbs_json_path = format!("{}/{}", username_path, "dbs.json");
        if !Path::new(&dbs_json_path).exists() {
            return Err(FileError::DbsJsonNotExists);
        }

        // username level check passed
        if db_name == None {
            return Ok(());
        }

        // check if db exists
        let dbs_file = fs::File::open(&dbs_json_path)?;
        let dbs_json: DbsJson = serde_json::from_reader(dbs_file)?;
        if !dbs_json
            .dbs
            .iter()
            .map(|db_info| db_info.name.clone())
            .collect::<Vec<String>>()
            .contains(&db_name.unwrap().to_string())
        {
            return Err(FileError::DbNotExists);
        }

        // check if db directory exists
        let db_path = format!("{}/{}", username_path, db_name.unwrap());
        if !Path::new(&db_path).exists() {
            return Err(FileError::DbDirNotExists);
        }

        // check if `tables.json` exists
        let tables_json_path = format!("{}/{}", db_path, "tables.json");
        if !Path::new(&tables_json_path).exists() {
            return Err(FileError::TablesJsonNotExists);
        }

        // db level check passed
        if table_name == None {
            return Ok(());
        }

        // check if table exists
        let tables_file = fs::File::open(&tables_json_path)?;
        let tables_json: TablesJson = serde_json::from_reader(tables_file)?;
        if !tables_json
            .tables
            .iter()
            .map(|table_info| table_info.name.clone())
            .collect::<Vec<String>>()
            .contains(&table_name.unwrap().to_string())
        {
            return Err(FileError::TableNotExists);
        }

        // check if table tsv exists
        let table_tsv_path = format!("{}/{}.tsv", db_path, table_name.unwrap());
        if !Path::new(&table_tsv_path).exists() {
            return Err(FileError::TableTsvNotExists);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    pub fn test_create_username() {
        let file_base_path = "data1";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }
        File::create_username("crazyguy", Some(file_base_path)).unwrap();
        File::create_username("happyguy", Some(file_base_path)).unwrap();

        assert!(Path::new(file_base_path).exists());

        let usernames_json_path = format!("{}/{}", file_base_path, "usernames.json");
        assert!(Path::new(&usernames_json_path).exists());

        let usernames_json = fs::read_to_string(usernames_json_path).unwrap();
        let usernames_json: UsernamesJson = serde_json::from_str(&usernames_json).unwrap();

        let ideal_usernames_json = UsernamesJson {
            usernames: vec![
                UsernameInfo {
                    name: "crazyguy".to_string(),
                    path: "crazyguy".to_string(),
                },
                UsernameInfo {
                    name: "happyguy".to_string(),
                    path: "happyguy".to_string(),
                },
            ],
        };

        assert_eq!(usernames_json.usernames[0].name, ideal_usernames_json.usernames[0].name);
        assert_eq!(usernames_json.usernames[1].name, ideal_usernames_json.usernames[1].name);
        assert_eq!(usernames_json.usernames[0].path, ideal_usernames_json.usernames[0].path);
        assert_eq!(usernames_json.usernames[1].path, ideal_usernames_json.usernames[1].path);

        assert!(Path::new(&format!("{}/{}", file_base_path, "crazyguy")).exists());
        assert!(Path::new(&format!("{}/{}", file_base_path, "happyguy")).exists());

        assert!(Path::new(&format!("{}/{}/{}", file_base_path, "crazyguy", "dbs.json")).exists());
        assert!(Path::new(&format!("{}/{}/{}", file_base_path, "happyguy", "dbs.json")).exists());

        let dbs_json = fs::read_to_string(format!("{}/{}/{}", file_base_path, "crazyguy", "dbs.json")).unwrap();
        let dbs_json: DbsJson = serde_json::from_str(&dbs_json).unwrap();

        assert_eq!(dbs_json.dbs.len(), 0);

        match File::create_username("happyguy", Some(file_base_path)) {
            Ok(_) => {}
            Err(e) => assert_eq!(
                format!("{}", e),
                "User name already exists and cannot be created again."
            ),
        };
    }

    #[test]
    pub fn test_get_usernames() {
        let file_base_path = "data2";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }
        File::create_username("crazyguy", Some(file_base_path)).unwrap();
        File::create_username("happyguy", Some(file_base_path)).unwrap();

        let usernames: Vec<String> = File::get_usernames(Some(file_base_path)).unwrap();
        assert_eq!(usernames, vec!["crazyguy", "happyguy"]);
    }

    #[test]
    pub fn test_remove_username() {
        let file_base_path = "data3";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }
        File::create_username("crazyguy", Some(file_base_path)).unwrap();
        File::create_username("happyguy", Some(file_base_path)).unwrap();
        File::create_username("sadguy", Some(file_base_path)).unwrap();

        let usernames: Vec<String> = File::get_usernames(Some(file_base_path)).unwrap();
        assert_eq!(usernames, vec!["crazyguy", "happyguy", "sadguy"]);

        File::remove_username("happyguy", Some(file_base_path)).unwrap();

        let usernames: Vec<String> = File::get_usernames(Some(file_base_path)).unwrap();
        assert_eq!(usernames, vec!["crazyguy", "sadguy"]);

        match File::remove_username("happyguy", Some(file_base_path)) {
            Ok(_) => {}
            Err(e) => assert_eq!(
                format!("{}", e),
                "Specified user name not exists. Please create this username first."
            ),
        };

        File::remove_username("sadguy", Some(file_base_path)).unwrap();

        let usernames: Vec<String> = File::get_usernames(Some(file_base_path)).unwrap();
        assert_eq!(usernames, vec!["crazyguy"]);

        File::remove_username("crazyguy", Some(file_base_path)).unwrap();

        let usernames: Vec<String> = File::get_usernames(Some(file_base_path)).unwrap();
        assert_eq!(usernames.len(), 0);
    }

    #[test]
    pub fn test_create_db() {
        let file_base_path = "data4";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }
        File::create_username("crazyguy", Some(file_base_path)).unwrap();
        File::create_db("crazyguy", "BookerDB", Some(file_base_path)).unwrap();
        File::create_db("crazyguy", "MovieDB", Some(file_base_path)).unwrap();

        let dbs_json_path = format!("{}/{}/{}", file_base_path, "crazyguy", "dbs.json");
        assert!(Path::new(&dbs_json_path).exists());

        let dbs_json = fs::read_to_string(dbs_json_path).unwrap();
        let dbs_json: DbsJson = serde_json::from_str(&dbs_json).unwrap();

        let ideal_dbs_json = DbsJson {
            dbs: vec![
                DbInfo {
                    name: "BookerDB".to_string(),
                    path: "BookerDB".to_string(),
                },
                DbInfo {
                    name: "MovieDB".to_string(),
                    path: "MovieDB".to_string(),
                },
            ],
        };

        assert_eq!(dbs_json.dbs[0].name, ideal_dbs_json.dbs[0].name);
        assert_eq!(dbs_json.dbs[1].name, ideal_dbs_json.dbs[1].name);
        assert_eq!(dbs_json.dbs[0].path, ideal_dbs_json.dbs[0].path);
        assert_eq!(dbs_json.dbs[1].path, ideal_dbs_json.dbs[1].path);

        assert!(Path::new(&format!("{}/{}/{}", file_base_path, "crazyguy", "BookerDB")).exists());
        assert!(Path::new(&format!("{}/{}/{}", file_base_path, "crazyguy", "MovieDB")).exists());

        assert!(Path::new(&format!(
            "{}/{}/{}/{}",
            file_base_path, "crazyguy", "BookerDB", "tables.json"
        ))
        .exists());
        assert!(Path::new(&format!(
            "{}/{}/{}/{}",
            file_base_path, "crazyguy", "MovieDB", "tables.json"
        ))
        .exists());

        let tables_json = fs::read_to_string(&format!(
            "{}/{}/{}/{}",
            file_base_path, "crazyguy", "BookerDB", "tables.json"
        ))
        .unwrap();
        let tables_json: TablesJson = serde_json::from_str(&tables_json).unwrap();

        assert_eq!(tables_json.tables.len(), 0);

        match File::create_db("happyguy", "BookerDB", Some(file_base_path)) {
            Ok(_) => {}
            Err(e) => assert_eq!(
                format!("{}", e),
                "Specified user name not exists. Please create this username first."
            ),
        };

        match File::create_db("crazyguy", "BookerDB", Some(file_base_path)) {
            Ok(_) => {}
            Err(e) => assert_eq!(format!("{}", e), "DB already exists and cannot be created again."),
        };
    }

    #[test]
    pub fn test_get_dbs() {
        let file_base_path = "data5";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }

        File::create_username("happyguy", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("happyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs.len(), 0);

        File::create_db("happyguy", "BookerDB", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("happyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs, vec!["BookerDB"]);

        File::create_db("happyguy", "MovieDB", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("happyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs, vec!["BookerDB", "MovieDB"]);

        match File::get_dbs("sadguy", Some(file_base_path)) {
            Ok(_) => {}
            Err(e) => assert_eq!(
                format!("{}", e),
                "Specified user name not exists. Please create this username first."
            ),
        };
    }

    #[test]
    pub fn test_remove_db() {
        let file_base_path = "data6";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }
        File::create_username("crazyguy", Some(file_base_path)).unwrap();
        File::create_db("crazyguy", "BookerDB", Some(file_base_path)).unwrap();
        File::create_db("crazyguy", "MovieDB", Some(file_base_path)).unwrap();
        File::create_db("crazyguy", "PhotoDB", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("crazyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs, vec!["BookerDB", "MovieDB", "PhotoDB"]);

        File::remove_db("crazyguy", "MovieDB", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("crazyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs, vec!["BookerDB", "PhotoDB"]);

        match File::remove_db("happyguy", "BookerDB", Some(file_base_path)) {
            Ok(_) => {}
            Err(e) => assert_eq!(
                format!("{}", e),
                "Specified user name not exists. Please create this username first."
            ),
        };

        File::remove_db("crazyguy", "PhotoDB", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("crazyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs, vec!["BookerDB"]);

        match File::remove_db("crazyguy", "PhotoDB", Some(file_base_path)) {
            Ok(_) => {}
            Err(e) => assert_eq!(format!("{}", e), "DB not exists. Please create DB first."),
        };

        File::remove_db("crazyguy", "BookerDB", Some(file_base_path)).unwrap();

        let dbs: Vec<String> = File::get_dbs("crazyguy", Some(file_base_path)).unwrap();
        assert_eq!(dbs.len(), 0);

        assert!(!Path::new(&format!("{}/{}/{}", file_base_path, "crazyguy", "BookerDB")).exists());
        assert!(!Path::new(&format!("{}/{}/{}", file_base_path, "crazyguy", "MovieDB")).exists());
        assert!(!Path::new(&format!("{}/{}/{}", file_base_path, "crazyguy", "PhotoDB")).exists());
    }

    #[test]
    pub fn test_create_load_drop_table() {
        let file_base_path = "data7";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }
        File::create_username("crazyguy", Some(file_base_path)).unwrap();
        File::create_db("crazyguy", "BookerDB", Some(file_base_path)).unwrap();

        let mut aff_table = Table::new("Affiliates");
        aff_table.fields.insert(
            "AffID".to_string(),
            Field::new_all("AffID", DataType::Int, true, None, field::Checker::None),
        );
        aff_table.fields.insert(
            "AffName".to_string(),
            Field::new_all("AffName", DataType::Varchar(40), true, None, field::Checker::None),
        );
        aff_table.fields.insert(
            "AffEmail".to_string(),
            Field::new_all("AffEmail", DataType::Varchar(50), true, None, field::Checker::None),
        );
        aff_table.fields.insert(
            "AffPhoneNum".to_string(),
            Field::new_all(
                "AffPhoneNum",
                DataType::Varchar(20),
                false,
                Some("+886900000000".to_string()),
                field::Checker::None,
            ),
        );
        aff_table.primary_key.push("AffID".to_string());

        File::create_table("crazyguy", "BookerDB", &aff_table, Some(file_base_path)).unwrap();

        let mut htl_table = Table::new("Hotels");
        htl_table.fields.insert(
            "HotelID".to_string(),
            Field::new_all("HotelID", DataType::Int, true, None, field::Checker::None),
        );
        htl_table.fields.insert(
            "HotelName".to_string(),
            Field::new_all("HotelName", DataType::Varchar(40), true, None, field::Checker::None),
        );
        htl_table.fields.insert(
            "HotelType".to_string(),
            Field::new_all(
                "HotelType",
                DataType::Varchar(20),
                false,
                Some("Homestay".to_string()),
                field::Checker::None,
            ),
        );
        htl_table.fields.insert(
            "HotelAddr".to_string(),
            Field::new_all(
                "HotelAddr",
                DataType::Varchar(50),
                false,
                Some("".to_string()),
                field::Checker::None,
            ),
        );
        htl_table.primary_key.push("HotelID".to_string());

        File::create_table("crazyguy", "BookerDB", &htl_table, Some(file_base_path)).unwrap();

        let ideal_tables = vec![
            TableInfo {
                name: "Affiliates".to_string(),
                path_tsv: "Affiliates.tsv".to_string(),
                path_bin: "Affiliates.bin".to_string(),
                primary_key: vec!["AffID".to_string()],
                foreign_key: vec![],
                reference_table: None,
                // reference_attr: None,
                last_rid: 0,
                // ignore attrs checking
                attrs_order: vec![],
                attrs: HashMap::new(),
            },
            TableInfo {
                name: "Hotels".to_string(),
                path_tsv: "Hotels.tsv".to_string(),
                path_bin: "Hotels.bin".to_string(),
                primary_key: vec!["HotelID".to_string()],
                foreign_key: vec![],
                reference_table: None,
                // reference_attr: None,
                last_rid: 0,
                // ignore attrs checking
                attrs_order: vec![],
                attrs: HashMap::new(),
            },
        ];

        let tables = File::load_tables("crazyguy", "BookerDB", Some(file_base_path)).unwrap();

        assert_eq!(tables.len(), 2);

        for i in 0..tables.len() {
            assert_eq!(tables[i].name, ideal_tables[i].name);
            assert_eq!(tables[i].path_tsv, ideal_tables[i].path_tsv);
            assert_eq!(tables[i].path_bin, ideal_tables[i].path_bin);
            assert_eq!(tables[i].primary_key, ideal_tables[i].primary_key);
            assert_eq!(tables[i].foreign_key, ideal_tables[i].foreign_key);
            assert_eq!(tables[i].reference_table, ideal_tables[i].reference_table);
            // assert_eq!(tables[i].reference_attr, ideal_tables[i].reference_attr);
            assert_eq!(tables[i].last_rid, ideal_tables[i].last_rid);
        }

        assert!(Path::new(&format!(
            "{}/{}/{}/{}",
            file_base_path, "crazyguy", "BookerDB", "Affiliates.tsv"
        ))
        .exists());
        assert!(Path::new(&format!(
            "{}/{}/{}/{}",
            file_base_path, "crazyguy", "BookerDB", "Hotels.tsv"
        ))
        .exists());

        let aff_tsv_content: Vec<String> = fs::read_to_string(&format!(
            "{}/{}/{}/{}",
            file_base_path, "crazyguy", "BookerDB", "Affiliates.tsv"
        ))
        .unwrap()
        .split('\t')
        .map(|s| s.to_string())
        .collect();

        assert_eq!(aff_tsv_content[0], "__rid__".to_string());
        assert_eq!(aff_tsv_content[1], "AffID".to_string());
        assert_eq!(aff_tsv_content.len(), 5);

        match File::create_table("happyguy", "BookerDB", &htl_table, Some(file_base_path)) {
            Ok(_) => {}
            Err(e) => assert_eq!(
                format!("{}", e),
                "Specified user name not exists. Please create this username first."
            ),
        };

        match File::create_table("crazyguy", "MusicDB", &htl_table, Some(file_base_path)) {
            Ok(_) => {}
            Err(e) => assert_eq!(format!("{}", e), "DB not exists. Please create DB first."),
        };

        match File::create_table("crazyguy", "BookerDB", &htl_table, Some(file_base_path)) {
            Ok(_) => {}
            Err(e) => assert_eq!(format!("{}", e), "Table already exists and cannot be created again."),
        };

        File::drop_table("crazyguy", "BookerDB", "Affiliates", Some(file_base_path)).unwrap();

        assert!(!Path::new(&format!(
            "{}/{}/{}/{}",
            file_base_path, "crazyguy", "BookerDB", "Affiliates.tsv"
        ))
        .exists());

        let tables = File::load_tables("crazyguy", "BookerDB", Some(file_base_path)).unwrap();

        assert_eq!(tables.len(), 1);

        match File::drop_table("crazyguy", "BookerDB", "Affiliates", Some(file_base_path)) {
            Ok(_) => {}
            Err(e) => assert_eq!(format!("{}", e), "Table not exists. Please create table first."),
        };

        File::drop_table("crazyguy", "BookerDB", "Hotels", Some(file_base_path)).unwrap();

        assert!(!Path::new(&format!(
            "{}/{}/{}/{}",
            file_base_path, "crazyguy", "BookerDB", "Hotels.tsv"
        ))
        .exists());

        let tables = File::load_tables("crazyguy", "BookerDB", Some(file_base_path)).unwrap();

        assert_eq!(tables.len(), 0);
    }
}
