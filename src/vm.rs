use std::{rc::Rc, sync::RwLock};

use super::compiler::{Statement, StatementType};
use super::cursor::Cursor;
use super::row::Row;
use super::row::TABLE_MAX_ROWS;
use super::table::Table;

pub fn do_meta_command(command: &str, table: Rc<RwLock<Table>>) -> Result<(), String> {
    if command == ".exit" {
        table.write().unwrap().close_db()?;
        std::process::exit(0);
    } else {
        Err(format!("Unrecognized Meta Command: {:?}", command))
    }
}

pub fn execute_statement(stmt: Statement, table: Rc<RwLock<Table>>) -> Result<(), String> {
    use StatementType::*;
    match stmt.type_ {
        Insert => execute_insert(stmt, table),
        Select => execute_select(stmt, table),
    }
}

fn execute_insert(stmt: Statement, table: Rc<RwLock<Table>>) -> Result<(), String> {
    if table.read().unwrap().num_rows >= TABLE_MAX_ROWS {
        return Err("Execute Table Full".to_owned());
    }
    let cursor = Cursor::table_end(table.clone());
    stmt.row_to_insert.unwrap().serialize(cursor.cursor_value());
    table.write().unwrap().num_rows += 1;
    Ok(())
}

fn execute_select(_stmt: Statement, table: Rc<RwLock<Table>>) -> Result<(), String> {
    let mut cursor = Cursor::table_start(table);
    while !cursor.end_of_table {
        let row = Row::deserialize(cursor.cursor_value());
        println!("{:?}", row);
        cursor.advance();
    }
    Ok(())
}
