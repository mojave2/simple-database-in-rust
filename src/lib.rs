pub mod compiler;
mod cursor;
mod pager;
mod row;
pub mod table;
pub mod vm;

use pager::{PAGE_SIZE, TABLE_MAX_PAGES};
use row::ROW_SIZE;

pub const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
pub const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;
