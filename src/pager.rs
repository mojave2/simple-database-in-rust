use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    rc::Rc,
    sync::RwLock,
};

use super::row::{ROWS_PER_PAGE, ROW_SIZE};

pub const PAGE_SIZE: usize = 4096;
pub const TABLE_MAX_PAGES: usize = 100;

pub type RowBuf = Rc<RwLock<Vec<u8>>>;
pub type Page = [Option<RowBuf>; ROWS_PER_PAGE];

pub struct Pager {
    file_handle: File,
    pub file_length: usize,
    pub pages: [Option<Page>; TABLE_MAX_PAGES],
}

impl Pager {
    pub fn open<P>(fname: P) -> Self
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

    pub fn get_page(&mut self, page_num: usize) -> &mut Page {
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

    pub fn flush(&mut self, page_num: usize, page_size: usize) -> Result<(), String> {
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

    pub fn close(&mut self) -> Result<(), String> {
        Ok(())
    }
}
