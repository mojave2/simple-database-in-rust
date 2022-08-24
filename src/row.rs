use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use std::io::{self, BufRead, Seek, SeekFrom, Write};

use super::compiler::PrepareError;
use super::pager::RowBuf;

const COLUMN_ID_SIZE: usize = std::mem::size_of::<u32>();
const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;
pub const ROW_SIZE: usize = COLUMN_ID_SIZE + COLUMN_EMAIL_SIZE + COLUMN_USERNAME_SIZE;

#[derive(Serialize, Deserialize, Debug)]
pub struct Row {
    id: u32,
    username: String,
    email: String,
}

impl Row {
    pub fn new(id: u32, username: &str, email: &str) -> Result<Self, PrepareError> {
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

    pub fn serialize(&self, buf: RowBuf) {
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

    pub fn deserialize(buf: RowBuf) -> Self {
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
