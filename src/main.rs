use std::io::{self, Write};

fn main() -> io::Result<()> {
    let mut buffer = String::new();
    loop {
        buffer.clear();
        print_prompt();
        io::stdin().read_line(&mut buffer)?;
        let command = buffer.trim();

        if command.chars().next().unwrap() == '.' {
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
                execute_statement(statement);
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
}

fn prepare_statement(command: &str) -> Result<Statement, String> {
    if &command[..6] == "insert" {
        Ok(Statement { type_: StatementType::Insert })
    } else if &command[..6] == "select" {
        Ok(Statement { type_: StatementType::Select })
    } else {
        Err(format!("Unrecognized Statement: {:?}", command))
    }
}

fn execute_statement(statement: Statement) {
    use StatementType::*;
    match statement.type_ {
        Insert => {
            println!("This is where we would do an insert.");
        }
        Select => {
            println!("This is where we would do a select.");
        }
    }
}
