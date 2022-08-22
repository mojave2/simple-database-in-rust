use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::io::{self, Write};

fn main() -> io::Result<()> {
    let mut table = Table::new();
    let mut buffer = String::new();
    loop {
        buffer.clear();
        print_prompt();
        io::stdin().read_line(&mut buffer)?;
        let command = buffer.trim();

        if command.starts_with('.') {
            match do_meta_command(command) {
                Ok(_) => continue,
                Err(e) => {
                    println!("{}", e);
                    continue;
                }
            }
        }

        match prepare_statement(command) {
            Ok(statement) => {
                match execute_statement(statement, &mut table) {
                    Ok(_) => (),
                    Err(e) => println!("{}", e),
                }
                println!("Executed.");
            }
            Err(e) => {
                println!("{}", e);
                continue;
            }
        }
    }
}

fn print_prompt() {
    print!("db> ");
    io::stdout().flush().unwrap();
}

fn do_meta_command(command: &str) -> Result<(), String> {
    if command == ".exit" {
        std::process::exit(0);
    } else {
        Err(format!("Unrecognized Meta Command: {:?}", command))
    }
}

enum StatementType {
    Insert,
    Select,
}

struct Statement {
    type_: StatementType,
    row_to_insert: Option<Row>,
}

fn prepare_statement(command: &str) -> Result<Statement, String> {
    let args: Vec<_> = command.split_whitespace().collect();
    if args[0] == "insert" {
        if args.len() != 4 {
            return Err(format!("Insert Command Argument Error: {:?}", command));
        }
        let id: u32 = match args[1].parse() {
            Ok(x) => x,
            Err(_) => return Err("Syntax error. Could not parse statement.".to_owned()),
        };
        let row_to_insert = Row {
            id,
            username: args[2].to_owned(),
            email: args[3].to_owned(),
        };
        Ok(Statement {
            type_: StatementType::Insert,
            row_to_insert: Some(row_to_insert),
        })
    } else if args[0] == "select" {
        Ok(Statement {
            type_: StatementType::Select,
            row_to_insert: None,
        })
    } else {
        Err(format!("Unrecognized Statement: {:?}", command))
    }
}

fn execute_statement(stmt: Statement, table: &mut Table) -> Result<(), String> {
    use StatementType::*;
    match stmt.type_ {
        Insert => execute_insert(stmt, table),
        Select => execute_select(stmt, table),
    }
}

fn execute_insert(stmt: Statement, table: &mut Table) -> Result<(), String> {
    if table.num_rows >= TABLE_MAX_ROWS {
        return Err("Execute Table Full".to_owned());
    }
    let row = table.row_slot(table.num_rows);
    if row.is_none() {
        *row = Some(BytesMut::with_capacity(ROW_SIZE));
    }
    stmt.row_to_insert.unwrap().serialize(row.as_mut().unwrap());
    table.num_rows += 1;
    Ok(())
}

fn execute_select(_stmt: Statement, table: &mut Table) -> Result<(), String> {
    for i in 0..table.num_rows {
        let row = Row::deserialize(table.row_slot(i).as_mut().unwrap());
        println!("{:?}", row);
    }
    Ok(())
}

//const COLUMN_USERNAME_SIZE: usize = 32;
//const COLUMN_EMAIL_SIZE: usize = 255;

#[derive(Serialize, Deserialize, Debug)]
struct Row {
    id: u32,
    username: String,
    email: String,
}

impl Row {
    fn serialize(&self, destination: &mut BytesMut) {
        let row_bin = bincode::serialize(&self).unwrap();
        destination.put(&row_bin[..]);
    }
    fn deserialize(destination: &mut BytesMut) -> Self {
        bincode::deserialize(destination).unwrap()
    }
}

const ROW_SIZE: usize = 291;
const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

struct Table {
    num_rows: usize,
    pages: [[Option<BytesMut>; ROWS_PER_PAGE]; TABLE_MAX_PAGES],
}

impl Table {
    fn new() -> Self {
        const INIT_ROW: Option<BytesMut> = None;
        const INIT_PAGE: [Option<BytesMut>; ROWS_PER_PAGE] = [INIT_ROW; ROWS_PER_PAGE];
        Table {
            num_rows: 0,
            pages: [INIT_PAGE; TABLE_MAX_PAGES],
        }
    }

    fn row_slot(&mut self, row_num: usize) -> &mut Option<BytesMut> {
        let page = &mut self.pages[row_num / ROWS_PER_PAGE];
        &mut page[row_num % ROWS_PER_PAGE]
    }
}

/*
## Refereces

- [Serialize into binary](https://rust-by-example-ext.com/serde/bincode.html)
- [crate bytes](https://docs.rs/bytes/latest/bytes/)
 */
