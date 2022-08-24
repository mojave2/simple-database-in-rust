use std::rc::Rc;
use std::sync::RwLock;

use super::pager::RowBuf;
use super::row::ROW_SIZE;
use super::table::Table;
use super::ROWS_PER_PAGE;

pub struct Cursor {
    pub table: Rc<RwLock<Table>>,
    pub row_num: usize,
    pub end_of_table: bool,
}

impl Cursor {
    pub fn table_start(table: Rc<RwLock<Table>>) -> Self {
        let num_rows = table.read().unwrap().num_rows;
        Self {
            table,
            row_num: 0,
            end_of_table: num_rows == 0,
        }
    }

    pub fn table_end(table: Rc<RwLock<Table>>) -> Self {
        let row_num = table.read().unwrap().num_rows;
        Self {
            table,
            row_num,
            end_of_table: true,
        }
    }

    pub fn cursor_value(&self) -> RowBuf {
        let mut table = self.table.write().unwrap();
        let page_num = self.row_num / ROWS_PER_PAGE;
        let page = table.pager.get_page(page_num);
        let row_offset = self.row_num % ROWS_PER_PAGE;
        if page[row_offset].is_none() {
            page[row_offset] = Some(Rc::new(RwLock::new(Vec::from([0; ROW_SIZE]))));
        }
        page[row_offset].as_ref().unwrap().clone()
    }

    pub fn advance(&mut self) {
        self.row_num += 1;
        if self.row_num >= self.table.read().unwrap().num_rows {
            self.end_of_table = true;
        }
    }
}
