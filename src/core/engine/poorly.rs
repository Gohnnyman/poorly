use crate::core::{
    database::{Database, DEFAULT_DB},
    schema::Columns,
    table::{self, Table},
};
use std::path::PathBuf;
use std::{collections::HashMap, hash::Hash};

use crate::core::types::{ColumnSet, PoorlyError, Query};

#[derive(Debug)]
pub struct Poorly {
    databases: HashMap<String, Database>,
    path: PathBuf,
}

impl Poorly {
    pub fn execute(&mut self, query: Query) -> Result<Vec<ColumnSet>, PoorlyError> {
        match query {
            Query::Select {
                db,
                from,
                columns,
                conditions,
            } => self.get_table(&db, &from)?.select(columns, conditions),
            Query::Insert { db, into, values } => {
                log::error!("ABOBA: {:#?}", values);
                self.get_table(&db, &into)?.insert(values).map(|v| vec![v])
            }
            Query::Update {
                db,
                table,
                set,
                conditions,
            } => self.get_table(&db, &table)?.update(set, conditions),
            Query::Delete {
                db,
                from,
                conditions,
            } => self.get_table(&db, &from)?.delete(conditions),
            Query::Create { db, table, columns } => {
                self.create_table(db, table, columns).map(|_| vec![])
            }
            Query::Drop { db, table } => self.drop_table(db, table).map(|_| vec![]),
            Query::DropDb { name } => {
                self.drop_db(name)?;
                Ok(vec![])
            }
            Query::CreateDb { name } => {
                self.create_db(name)?;
                Ok(vec![])
            }
            Query::Alter { db, table, rename } => {
                self.alter_table(db, table, rename)?;
                Ok(vec![])
            }
            Query::Join {
                table1,
                table2,
                columns,
            } => {
                todo!()
            }
        }
    }

    pub fn drop_table(&mut self, db: String, table_name: String) -> Result<(), PoorlyError> {
        let db = self.get_database(&db)?;

        db.drop_table(table_name)
    }

    pub fn drop_db(&mut self, name: String) -> Result<(), PoorlyError> {
        let db = self.get_database(&name)?;
        db.drop_db()?;

        self.databases.remove(&name);

        log::info!("Database {} dropped", name);

        Ok(())
    }

    pub fn alter_table(
        &mut self,
        db: String,
        table_name: String,
        rename: HashMap<String, String>,
    ) -> Result<(), PoorlyError> {
        let db = self.get_database(&db)?;

        db.alter_table(table_name, rename)
    }

    pub fn create_table(
        &mut self,
        db: String,
        table_name: String,
        columns: Columns,
    ) -> Result<(), PoorlyError> {
        let db = self.get_database(&db)?;
        db.create_table(table_name, columns)
    }

    fn get_database(&mut self, db_name: &str) -> Result<&mut Database, PoorlyError> {
        if !self.databases.contains_key(db_name) {
            let db = Database::open(db_name, self.path.clone())?;
            self.databases.insert(db_name.to_string(), db);
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

    fn get_table(&mut self, db: &str, name: &str) -> Result<&mut Table, PoorlyError> {
        self.get_database(db)?.get_table(name)
    }
}
