use std::io::{self, Write};

fn main() -> io::Result<()> {
    let mut buffer = String::new();
    loop {
        buffer.clear();
        print_prompt();
        io::stdin().read_line(&mut buffer)?;
        let command = buffer.trim();

        if command == ".exit" {
            std::process::exit(0);
        } else {
            println!("Unrecognized command: {:?}.", command);
        }
    }
}

fn print_prompt() {
    print!("db> ");
    //let mut stdout = io::stdout();
    //write!(stdout, "db> ");
    io::stdout().flush().unwrap();
}

