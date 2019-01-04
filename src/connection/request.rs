use crate::sql::parser::{Parser, ParserError};
use crate::sql::worker::{SQL, SQLError};
use crate::manager::pool::{Pool, PoolError};
use crate::Response;
use std::fmt;

use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct Request {}

#[derive(Debug)]
pub enum RequestError {
    PoolError(PoolError),
    CauseByParser(ParserError),
    UserNotExist(String),
    // DBNotExist(String),
    CreateDBBeforeCmd,
    BadRequest,
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RequestError::PoolError(ref e) => write!(f, "error caused by pool: {}", e),
            RequestError::CauseByParser(ref e) => write!(f, "error caused by parser: {}", e),
            RequestError::UserNotExist(ref s) => write!(f, "user: {} not found", s),
            // RequestError::DBNotExist(ref s) => write!(f, "database: {} not found", s),
            RequestError::CreateDBBeforeCmd => write!(f, "please create a database before any other commands"),
            RequestError::BadRequest => write!(f, "BadRequest, invalid request format"),
        }
    }
}

impl Request {
    pub fn parse(input: &str, mut ssssql: &mut SQL, mutex: &Arc<Mutex<Pool>>, addr: String) -> Result<Response, RequestError> {
        /*
         * request format
         * case1:
         * username||databasename||command;
         * case2:
         * username||||create dbname;
         *
         */
        println!("{}", addr);
        let split_str: Vec<&str> = input.split("||").collect();
        if split_str.len() != 3 {
            return Err(RequestError::BadRequest);
        }

        let username = split_str[0];
        let dbname = split_str[1];
        let cmd = format!("{};", split_str[2]);

        // initialize username
        // TODO: where to place user verification ??

        // load sql object from memory pool
        let mut pool = mutex.lock().unwrap();
        let mut sql = match pool.get(username, dbname, addr) {
            Ok(tsql) => tsql,
            Err(ret) => return Err(RequestError::PoolError(ret)),
        };
        // check dbname
        if dbname != "" {
            let parser = Parser::new(&cmd).unwrap();
            match parser.parse(&mut sql) {
                Err(ret) => return Err(RequestError::CauseByParser(ret)),
                Ok(_) => {}
            }
        } else {
            // check cmd if it is "create database dbname;"
            let mut iter = cmd.split_whitespace();
            if iter.next() != Some("create") || iter.next() != Some("database") {
                return Err(RequestError::CreateDBBeforeCmd);
            }
            let parser = Parser::new(&cmd).unwrap();
            match parser.parse(&mut sql) {
                Err(ret) => return Err(RequestError::CauseByParser(ret)),
                Ok(_) => {}
            }
        }
        Ok(Response::OK {
            msg: "Query OK!".to_string(),
        })
        //Ok(Response::OK { msg: format!("{}, user:{}",input, sql.username) })
    }
    fn user_verify(name: &str) -> Result<(), ()> {
        // TODO: auto create new users
        if name == "" {
            return Err(());
        }
        Ok(())
    }
}
