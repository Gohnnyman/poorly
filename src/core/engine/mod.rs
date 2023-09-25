use super::schema::Schema;
use super::types::{ColumnSet, PoorlyError, Query};
use std::sync::Mutex;

pub mod poorly;

pub trait DatabaseEng: Send + Sync {
    fn execute(&self, query: Query) -> Result<Vec<ColumnSet>, PoorlyError>;
}

impl DatabaseEng for Mutex<poorly::Poorly> {
    fn execute(&self, query: Query) -> Result<Vec<ColumnSet>, PoorlyError> {
        self.lock().unwrap().execute(query)
    }
}
