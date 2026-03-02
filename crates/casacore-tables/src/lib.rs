pub mod table;
mod table_impl;

pub(crate) mod aipsio;
pub(crate) mod storage;

pub use table::{Table, TableError, TableOptions};
