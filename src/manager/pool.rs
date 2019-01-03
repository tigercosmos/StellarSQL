use crate::sql::worker::SQLError;
use crate::sql::worker::SQL;
use std::fmt;

use std::sync::{Arc, Mutex};
use std::collections::{BTreeMap, VecDeque};

/*
 * freelist: [recent use ..... least recent use]
 */
#[derive(Debug)]
pub struct Pool {
    pub max_entry: usize,
    pub freelist: VecDeque<String>,
    pub cache: BTreeMap<String, SQL>,
}

#[derive(Debug)]
pub enum PoolError {
    SQLError(SQLError),
    EntryNotExist(String),
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PoolError::SQLError(ref e) => write!(f, "error cause by worker: {}", e),
            PoolError::EntryNotExist(ref s) => write!(f, "user: {} entry is not existed", s),
        }
    }
}
// TODO: mutex
impl Pool {
    pub fn new(entry_number: usize) -> Result<Pool, PoolError> {
        Ok(Pool {
            max_entry: entry_number,
            freelist: VecDeque::new(),
            cache: BTreeMap::new(),
        })
    }
    pub fn get(&mut self, username: &str, dbname: &str) -> Result<&SQL, PoolError> {
        // get username entry from cache, if entry is not existed, load from disk
        // TODO: validation: check dbname with sql.database.name
        if !self.cache.contains_key(username) {
            // if sql is not in cache, load from disk and add the sql object into cache
            let mut sql = SQL::new(username).unwrap();
            match sql.load_database(dbname) {
                Ok(e) => {},
                Err(ret) => return Err(PoolError::SQLError(ret)),
            }
            match self.insert(sql) {
                Ok(e) => {},
                Err(ret) => return Err(ret),
            }
        }
        // if username entry is not in freelist[0](most recent use), move it to [0]
        if self.freelist[0] != username {
            let l = self.freelist.len();
            for i in 1..l {
                if self.freelist[i] == username {
                    self.freelist.remove(i);
                    let key = username.clone();
                    self.freelist.push_front(key.to_string());
                    break;
                }
            }
        }
        Ok(self.cache.get(username).unwrap())
    }
    pub fn insert(&mut self, sql: SQL) -> Result<(), PoolError> {
        // check cache size, pop and write back Least Recent Use(LRU) entry
        if self.cache.len() >= self.max_entry {
            let user = self.freelist.pop_back().unwrap();
            match self.write_back(&user) {
                Ok(e) => {},
                Err(ret) => return Err(ret),
            }
        }
        let user = sql.username.clone();
        let key = sql.username.clone();
        self.cache.insert(user, sql);
        self.freelist.push_front(key);
        Ok(())
    }
    pub fn write_back(&mut self, username: &str) -> Result<(), PoolError> {
        // pop username entry, write this entry back to disk

        // pop from freelist
        let l = self.freelist.len();
        for i in 0..l {
            if self.freelist[i] == username {
                self.freelist.remove(i);
                break;
            }
        }
        let sql = match self.cache.get(username) {
            Some(tsql) => tsql,
            None => return Err( PoolError::EntryNotExist(username.to_string()) ),
        };
        // write back
        // remove from cache
        self.cache.remove(username);
        Ok(())
    }
}
