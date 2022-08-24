use std::io::{self, Write};

use simple_database_in_rust::compiler;
use simple_database_in_rust::table::Table;
use simple_database_in_rust::vm;

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
            match vm::do_meta_command(command, table.clone()) {
                Ok(_) => continue,
                Err(e) => {
                    println!("{}", e);
                    continue;
                }
            }
        }

        match compiler::prepare_statement(command) {
            Ok(statement) => {
                match vm::execute_statement(statement, table.clone()) {
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

/*
## Refereces

- [Serialize into binary](https://rust-by-example-ext.com/serde/bincode.html)
- [crate bytes](https://docs.rs/bytes/latest/bytes/)
 */
