use crate::Response;
use crate::sql::worker::SQL;
use crate::sql::worker::SQLError;
use crate::sql::parser::Parser;
use crate::sql::parser::ParserError;
use std::fmt;


#[derive(Debug)]
pub struct Request {
}


#[derive(Debug)]
pub enum RequestMsg {
    SQLError(SQLError),
    CauseByParser(ParserError),
    UserNotExist(String),
    DBNotExist(String),
    CreateDBBeforeCmd,
    BadRequest,
    QueryOK,
}

impl fmt::Display for RequestMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RequestMsg::SQLError(ref e) => write!(f, "error caused by worker: {}", e),
            RequestMsg::CauseByParser(ref e) => write!(f, "error caused by parser: {}", e),
            RequestMsg::UserNotExist(ref s) => write!(f, "user: {} not found", s),
            RequestMsg::DBNotExist(ref s) => write!(f, "database: {} not found", s),
            RequestMsg::CreateDBBeforeCmd => write!(f, "please create a database before any other commands"),
            RequestMsg::BadRequest => write!(f, "BadRequest, invalid request format"),
            RequestMsg::QueryOK => write!(f, "Query OK!"),
        }
    }
}


impl Request {
    pub fn parse(input: &str, mut sql: &mut SQL) -> Result<Response, String> {
        /*
         * request format
         * case1: 
         * username||databasename||command;
         * case2:
         * username||||create dbname;
         *
         */
        let mut split_str: Vec<&str>= input.split("||").collect();
        if split_str.len() != 3 {
            return Err( format!("{}", RequestMsg::BadRequest) );
        }

        let username = split_str[0];
        let dbname = split_str[1];
        let cmd = format!("{};", split_str[2]);

        // initialize username
        if sql.username == "" {
            if Request::user_verify(username).is_ok() {
                sql.username = username.to_string();
            }else{
                // user not existed
                return Err( format!("{}", RequestMsg::UserNotExist(username.to_string())));
            }
        }

        // check dbname
        if dbname != "" {
            if sql.database.name == "" {
                match sql.load_database(dbname) {
                    Err(ret) => return Err( format!("{}", RequestMsg::SQLError(ret)) ),
                    Ok(e) => {},
                }
            }
            let parser = Parser::new(&cmd).unwrap();
            match parser.parse(&mut sql) {
                Err(ret) => return Err( format!("{}", RequestMsg::CauseByParser(ret)) ), 
                Ok(e) => {},
            }
        }else{
            // check cmd if it is "create database dbname;"
            let mut iter = cmd.split_whitespace();
            if iter.next() != Some("create") || iter.next() != Some("database"){
                return Err( format!("{}", RequestMsg::CreateDBBeforeCmd) );
            }
            let parser = Parser::new(&cmd).unwrap();
            match parser.parse(&mut sql) {
                Err(ret) => return Err( format!("{}", RequestMsg::CauseByParser(ret)) ), 
                Ok(e) => {},
            }

        }
        Ok(Response::OK { msg: format!("{}", RequestMsg::QueryOK) })
        //Ok(Response::OK { msg: format!("{}, user:{}",input, sql.username) })
    }
    fn user_verify(name: &str) -> Result<(), ()> {
        if name != "" {
            return Ok(());
        }else{
            return Err(());
        }
        Ok(())
    }
}
