use super::row::Row;

pub enum StatementType {
    Insert,
    Select,
}

pub struct Statement {
    pub type_: StatementType,
    pub row_to_insert: Option<Row>,
}

#[derive(Debug)]
pub enum PrepareError {
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

pub fn prepare_statement(command: &str) -> Result<Statement, PrepareError> {
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
