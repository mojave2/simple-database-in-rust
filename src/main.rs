use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{self, BufRead, Read, Seek, SeekFrom, Write},
    path::Path,
    rc::Rc,
    sync::RwLock,
};

fn main() -> io::Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Must supply a database filename.");
        std::process::exit(-1);
    }
    let filename = &args[1];
    let table = Table::open_db(filename);
    let mut buffer = String::new();
    loop {
        buffer.clear();
        print_prompt();
        io::stdin().read_line(&mut buffer)?;
        let command = buffer.trim();

        if command.starts_with('.') {
            match do_meta_command(command, table.clone()) {
                Ok(_) => continue,
                Err(e) => {
                    println!("{}", e);
                    continue;
                }
            }
        }

        match prepare_statement(command) {
            Ok(statement) => {
                match execute_statement(statement, table.clone()) {
                    Ok(_) => (),
                    Err(e) => println!("{}", e),
                }
                println!("Executed.");
            }
            Err(e) => {
                println!("{:?}", e);
                continue;
            }
        }
    }
}

fn print_prompt() {
    print!("db> ");
    io::stdout().flush().unwrap();
}

fn do_meta_command(command: &str, table: Rc<RwLock<Table>>) -> Result<(), String> {
    if command == ".exit" {
        table.write().unwrap().close_db()?;
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

#[derive(Debug)]
enum PrepareError {
    StringTooLong(String),
    SyntaxError(String),
    UnrecognizedStatement(String),
}

fn prepare_insert(command: &str) -> Result<Statement, PrepareError> {
    use PrepareError::*;
    let args: Vec<_> = command.split_whitespace().collect();
    if args.len() != 4 {
        return Err(SyntaxError(format!(
            "Insert Command Argument Error: {:?}",
            command
        )));
    }
    let id: u32 = args[1]
        .parse::<u32>()
        .map_err(|_| SyntaxError("Syntax error. Could not parse statement.".to_owned()))?;
    let row_to_insert = Row::new(id, args[2], args[3])?;
    Ok(Statement {
        type_: StatementType::Insert,
        row_to_insert: Some(row_to_insert),
    })
}

fn prepare_statement(command: &str) -> Result<Statement, PrepareError> {
    use PrepareError::*;
    let args: Vec<_> = command.split_whitespace().collect();
    if args[0] == "insert" {
        prepare_insert(command)
    } else if args[0] == "select" {
        Ok(Statement {
            type_: StatementType::Select,
            row_to_insert: None,
        })
    } else {
        Err(UnrecognizedStatement(format!(
            "Unrecognized Statement: {:?}",
            command
        )))
    }
}

fn execute_statement(stmt: Statement, table: Rc<RwLock<Table>>) -> Result<(), String> {
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

const COLUMN_ID_SIZE: usize = std::mem::size_of::<u32>();
const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;

#[derive(Serialize, Deserialize, Debug)]
struct Row {
    id: u32,
    username: String,
    email: String,
}

impl Row {
    fn new(id: u32, username: &str, email: &str) -> Result<Self, PrepareError> {
        use PrepareError::*;
        if username.len() > COLUMN_USERNAME_SIZE - 1 {
            return Err(StringTooLong(format!(
                "username string is too long. (>{})",
                COLUMN_USERNAME_SIZE - 1
            )));
        }
        if email.len() > COLUMN_EMAIL_SIZE - 1 {
            return Err(StringTooLong(format!(
                "email string is too long. (>{})",
                COLUMN_EMAIL_SIZE - 1
            )));
        }
        Ok(Row {
            id,
            username: username.to_owned(),
            email: email.to_owned(),
        })
    }

    fn serialize(&self, buf: RowBuf) {
        let buf_arr = &mut buf.write().unwrap()[..];
        let mut cur = io::Cursor::new(buf_arr);
        cur.write_u32::<LittleEndian>(self.id).unwrap();
        cur.seek(SeekFrom::Start(COLUMN_ID_SIZE as u64)).unwrap();
        let _ = cur.write(self.username.as_bytes()).unwrap();
        cur.seek(SeekFrom::Start(
            (COLUMN_ID_SIZE + COLUMN_USERNAME_SIZE) as u64,
        ))
        .unwrap();
        let _ = cur.write(self.email.as_bytes()).unwrap();
    }

    fn deserialize(buf: RowBuf) -> Self {
        let buf_arr = &buf.read().unwrap()[..];
        let mut cur = io::Cursor::new(buf_arr);
        let id = cur.read_u32::<LittleEndian>().unwrap();
        cur.seek(SeekFrom::Start(COLUMN_ID_SIZE as u64)).unwrap();
        let mut username: Vec<u8> = Vec::new();
        cur.read_until(0, &mut username).unwrap();
        let username = &username[..username.len() - 1];
        cur.seek(SeekFrom::Start(
            (COLUMN_ID_SIZE + COLUMN_USERNAME_SIZE) as u64,
        ))
        .unwrap();
        let mut email: Vec<u8> = Vec::new();
        cur.read_until(0, &mut email).unwrap();
        let email = &email[..email.len() - 1];
        Self {
            id,
            username: String::from_utf8_lossy(username).into(),
            email: String::from_utf8_lossy(email).into(),
        }
    }
}

const ROW_SIZE: usize = std::mem::size_of::<u32>() + COLUMN_EMAIL_SIZE + COLUMN_USERNAME_SIZE;
const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

type RowBuf = Rc<RwLock<Vec<u8>>>;
type Page = [Option<RowBuf>; ROWS_PER_PAGE];

struct Table {
    num_rows: usize,
    pager: Pager,
}

impl Table {
    fn open_db<P>(fname: P) -> Rc<RwLock<Self>>
    where
        P: AsRef<Path>,
    {
        let pager = Pager::open(fname);
        let num_rows = pager.file_length / ROW_SIZE;
        Rc::new(RwLock::new(Table { num_rows, pager }))
    }

    fn close_db(&mut self) -> Result<(), String> {
        let pager = &mut self.pager;
        let num_full_pages = self.num_rows / ROWS_PER_PAGE;
        for i in 0..num_full_pages {
            if pager.pages[i].is_some() {
                pager.flush(i, PAGE_SIZE)?;
                pager.pages[i] = None;
            }
        }

        let num_additional_rows = self.num_rows % ROWS_PER_PAGE;
        if num_additional_rows > 0 {
            let page_num = num_full_pages;
            if pager.pages[page_num].is_some() {
                pager.flush(page_num, num_additional_rows * ROW_SIZE)?;
                pager.pages[page_num] = None;
            }
        }

        self.pager.close()?;
        Ok(())
    }
}

struct Pager {
    file_handle: File,
    file_length: usize,
    pages: [Option<Page>; TABLE_MAX_PAGES],
}

impl Pager {
    fn open<P>(fname: P) -> Self
    where
        P: AsRef<Path>,
    {
        let file_handle = OpenOptions::new()
            .write(true)
            .read(true)
            .open(fname)
            .expect("Unable to open file.");
        let file_length = file_handle.metadata().unwrap().len() as usize;
        const INIT_PAGE: Option<Page> = None;
        Self {
            file_handle,
            file_length,
            pages: [INIT_PAGE; TABLE_MAX_PAGES],
        }
    }

    fn get_page(&mut self, page_num: usize) -> &mut Page {
        if page_num > TABLE_MAX_PAGES {
            eprintln!("Tried to fetch page number out of bounds. {}", page_num);
            std::process::exit(-1);
        }
        if self.pages[page_num].is_none() {
            const INIT_ROW: Option<RowBuf> = None;
            self.pages[page_num] = Some([INIT_ROW; ROWS_PER_PAGE]);

            let mut num_pages = self.file_length / PAGE_SIZE;
            // we might save a partial page at the end of the file
            if self.file_length % PAGE_SIZE > 0 {
                num_pages += 1;
            }
            if page_num <= num_pages {
                self.file_handle
                    .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                    .unwrap();
                let mut page_buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
                let _ = self.file_handle.read(&mut page_buf[..]).unwrap();
                for i in 0..ROWS_PER_PAGE {
                    let r = &page_buf[i * ROW_SIZE..(i + 1) * ROW_SIZE];
                    if r != [0; ROW_SIZE] {
                        let p = self.pages[page_num].as_mut().unwrap();
                        p[i] = Some(Rc::new(RwLock::new(Vec::from(r))));
                    }
                }
            }
        }
        self.pages[page_num].as_mut().unwrap()
    }

    fn flush(&mut self, page_num: usize, page_size: usize) -> Result<(), String> {
        self.file_handle
            .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
            .expect("Error seeking offset");
        let mut page_buf = [0; PAGE_SIZE];
        for i in 0..(page_size / ROW_SIZE) {
            let p = self.pages[page_num].as_ref().unwrap();
            if p[i].is_some() {
                let r = p[i].as_ref().unwrap();
                let arr = r.read().unwrap();
                page_buf[i * ROW_SIZE..(i + 1) * ROW_SIZE].copy_from_slice(&arr[..]);
            }
        }
        self.file_handle
            .write(&page_buf[..page_size])
            .map_err(|_| "Error writing page buffer".to_owned())?;
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        Ok(())
    }
}

struct Cursor {
    table: Rc<RwLock<Table>>,
    row_num: usize,
    end_of_table: bool,
}

impl Cursor {
    fn table_start(table: Rc<RwLock<Table>>) -> Self {
        let num_rows = table.read().unwrap().num_rows;
        Self {
            table,
            row_num: 0,
            end_of_table: num_rows == 0,
        }
    }

    fn table_end(table: Rc<RwLock<Table>>) -> Self {
        let row_num = table.read().unwrap().num_rows;
        Self {
            table,
            row_num,
            end_of_table: true,
        }
    }

    fn cursor_value(&self) -> RowBuf {
        let mut table = self.table.write().unwrap();
        let page_num = self.row_num / ROWS_PER_PAGE;
        let page = table.pager.get_page(page_num);
        let row_offset = self.row_num % ROWS_PER_PAGE;
        if page[row_offset].is_none() {
            page[row_offset] = Some(Rc::new(RwLock::new(Vec::from([0; ROW_SIZE]))));
        }
        page[row_offset].as_ref().unwrap().clone()
    }

    fn advance(&mut self) {
        self.row_num += 1;
        if self.row_num >= self.table.read().unwrap().num_rows {
            self.end_of_table = true;
        }
    }
}

/*
## Refereces

- [Serialize into binary](https://rust-by-example-ext.com/serde/bincode.html)
- [crate bytes](https://docs.rs/bytes/latest/bytes/)
 */
