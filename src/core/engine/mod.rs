use super::types::{ColumnSet, PoorlyError, Query};
use async_trait::async_trait;
use tokio::sync::Mutex;

pub mod poorly;

#[async_trait]
pub trait DatabaseEng: Send + Sync {
    async fn execute(&self, query: Query) -> Result<Vec<ColumnSet>, PoorlyError>;
}

#[async_trait]
impl DatabaseEng for Mutex<poorly::Poorly> {
    async fn execute(&self, query: Query) -> Result<Vec<ColumnSet>, PoorlyError> {
        let mut lock = self.lock().await;

        let tmp = lock.execute(query).await;

        tmp
    }
}
