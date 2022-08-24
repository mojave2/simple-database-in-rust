use std::{path::Path, rc::Rc, sync::RwLock};

use super::pager::Pager;
use super::pager::PAGE_SIZE;
use super::row::ROW_SIZE;
use super::ROWS_PER_PAGE;

pub struct Table {
    pub num_rows: usize,
    pub pager: Pager,
}

impl Table {
    pub fn open_db<P>(fname: P) -> Rc<RwLock<Self>>
    where
        P: AsRef<Path>,
    {
        let pager = Pager::open(fname);
        let num_rows = pager.file_length / ROW_SIZE;
        Rc::new(RwLock::new(Table { num_rows, pager }))
    }

    pub fn close_db(&mut self) -> Result<(), String> {
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
