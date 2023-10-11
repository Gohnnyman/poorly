use tokio::sync::{Mutex, RwLock};

use crate::core::{
    database::{Database, DEFAULT_DB},
    schema::Columns,
    table::Table,
    types::TypedValue,
};
use std::{collections::HashMap, hash::Hash};
use std::{path::PathBuf, sync::Arc};

use crate::core::types::{ColumnSet, PoorlyError, Query};

#[derive(Debug)]
pub struct Poorly {
    databases: HashMap<String, RwLock<Database>>,
    path: PathBuf,
}

impl Poorly {
    pub async fn execute(&mut self, query: Query) -> Result<Vec<ColumnSet>, PoorlyError> {
        match query {
            Query::Select {
                db,
                from,
                columns,
                conditions,
            } => self
                .get_table(&db, &from)
                .await?
                .write()
                .await
                .select(columns, conditions),
            Query::Insert { db, into, values } => self
                .get_table(&db, &into)
                .await?
                .write()
                .await
                .insert(values)
                .map(|v| vec![v]),
            Query::Update {
                db,
                table,
                set,
                conditions,
            } => self
                .get_table(&db, &table)
                .await?
                .write()
                .await
                .update(set, conditions),
            Query::Delete {
                db,
                from,
                conditions,
            } => self
                .get_table(&db, &from)
                .await?
                .write()
                .await
                .delete(conditions),
            Query::Create { db, table, columns } => {
                self.create_table(db, table, columns).await.map(|_| vec![])
            }
            Query::Drop { db, table } => self.drop_table(db, table).await.map(|_| vec![]),
            Query::DropDb { name } => {
                self.drop_db(name).await?;
                Ok(vec![])
            }
            Query::CreateDb { name } => {
                self.create_db(name)?;
                Ok(vec![])
            }
            Query::Alter { db, table, rename } => {
                self.alter_table(db, table, rename).await?;
                Ok(vec![])
            }
            Query::ShowTables { db } => {
                let db = self.get_database(&db).await?;
                let tables: ColumnSet = db
                    .read()
                    .await
                    .get_tables()
                    .into_iter()
                    .map(|t| (t, TypedValue::String("".to_string())))
                    .collect();

                Ok(vec![tables])
            }
            Query::Join {
                db,
                table1,
                table2,
                columns,
                conditions,
                join_on,
            } => {
                let result = self
                    .join(db, table1, table2, columns, conditions, join_on)
                    .await?;

                Ok(result)
            }
        }
    }

    pub async fn join(
        &mut self,
        db: String,
        table1: String,
        table2: String,
        columns: Vec<String>,
        conditions: HashMap<String, TypedValue>,
        join_on: HashMap<String, String>,
    ) -> Result<Vec<ColumnSet>, PoorlyError> {
        let t1 = self.get_table(&db, &table1).await?;
        let mut t1 = t1.write().await;

        let t2 = self.get_table(&db, &table2).await?;
        let mut t2 = t2.write().await;

        let result = t1.join(&mut t2, columns, conditions, join_on)?;

        Ok(result)
    }

    pub async fn drop_table(&mut self, db: String, table_name: String) -> Result<(), PoorlyError> {
        let mut db = self.get_database(&db).await?.write().await;

        db.drop_table(table_name).await
    }

    pub async fn drop_db(&mut self, name: String) -> Result<(), PoorlyError> {
        let mut db = self.get_database(&name).await?.write().await;
        db.drop_db()?;

        drop(db);

        self.databases.remove(&name);

        log::info!("Database {} dropped", name);

        Ok(())
    }

    pub async fn alter_table(
        &mut self,
        db: String,
        table_name: String,
        rename: HashMap<String, String>,
    ) -> Result<(), PoorlyError> {
        let mut db = self.get_database(&db).await?.write().await;

        db.alter_table(table_name, rename).await
    }

    pub async fn create_table(
        &mut self,
        db: String,
        table_name: String,
        columns: Columns,
    ) -> Result<(), PoorlyError> {
        let mut db = self.get_database(&db).await?.write().await;
        db.create_table(table_name, columns)
    }

    async fn get_database(&mut self, db_name: &str) -> Result<&RwLock<Database>, PoorlyError> {
        if !self.databases.contains_key(db_name) {
            let db = Database::open(db_name, self.path.clone())?;
            self.databases.insert(db_name.to_string(), RwLock::new(db));
        };

        let database = self.databases.get_mut(db_name).unwrap();

        Ok(database)
    }

    pub fn open(path: PathBuf) -> Self {
        log::info!("Opening server folder at {:?}", path);
        if !path.is_dir() && path.exists() {
            panic!("Server folder not found at {:?}", path);
        }

        Poorly {
            databases: HashMap::new(),
            path,
        }
    }

    pub fn init(&self) -> Result<(), PoorlyError> {
        if self.path.join(DEFAULT_DB).exists() {
            return Ok(());
        }

        self.create_db(DEFAULT_DB.to_string())
    }

    pub fn create_db(&self, name: String) -> Result<(), PoorlyError> {
        log::info!("Creating database {} at {:?}", name, self.path);
        Database::create_db(name, self.path.clone())
    }

    async fn get_table(&mut self, db: &str, name: &str) -> Result<Arc<RwLock<Table>>, PoorlyError> {
        let mut db = self.get_database(db).await?.write().await;
        let tmp = db.get_table(name).await;

        tmp
    }
}
