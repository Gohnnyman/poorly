use super::schema::{Columns, Schema};
use super::table::Table;
use super::types::PoorlyError;

use std::collections::HashMap;
use std::path::PathBuf;

#[cfg(test)]
mod tests;

pub const DEFAULT_DB: &'static str = "poorly";

#[derive(Debug)]
pub struct Database {
    tables: HashMap<String, Table>,
    schema: Schema,
    path: PathBuf,
}

// TODO: add cleanup (remove all deleted entries)
impl Database {
    pub fn drop_table(&mut self, table_name: String) -> Result<(), PoorlyError> {
        self.schema.drop_table(table_name.clone())?;
        self.get_table(&table_name)?.drop()?;
        self.tables.remove(&table_name);

        Ok(())
    }

    pub fn create_table(
        &mut self,
        table_name: String,
        columns: Columns,
    ) -> Result<(), PoorlyError> {
        self.schema.create_table(table_name, columns)
    }

    pub fn alter_table(
        &mut self,
        table_name: String,
        rename: HashMap<String, String>,
    ) -> Result<(), PoorlyError> {
        self.schema.alter_table(table_name.clone(), rename)?;

        self.update_colunms(table_name);

        Ok(())
    }

    fn update_colunms(&mut self, table: String) {
        self.tables
            .entry(table.clone())
            .and_modify(|e| e.columns = self.schema.tables[&table].clone());
    }

    pub fn create_db(db_name: String, mut path: PathBuf) -> Result<(), PoorlyError> {
        path.push(db_name.clone());

        if path.exists() {
            return Err(PoorlyError::DatabaseAlreadyExists(db_name.clone()));
        }

        std::fs::create_dir_all(&path)?;

        let schema = Schema::new_poorly(db_name);
        schema.dump(path.as_path())?;

        Ok(())
    }

    pub fn drop_db(&mut self) -> Result<(), PoorlyError> {
        if self.path.file_name().unwrap() != DEFAULT_DB {
            std::fs::remove_dir_all(&self.path)?;
        } else {
            return Err(PoorlyError::CannotDropDefaultDb);
        }

        Ok(())
    }

    pub fn get_table(&mut self, table_name: &str) -> Result<&mut Table, PoorlyError> {
        if !self.schema.tables.contains_key(table_name) {
            return Err(PoorlyError::TableNotFound(table_name.to_string()));
        }

        if !self.tables.contains_key(table_name) {
            let columns = self.schema.tables[table_name].clone();
            let table = Table::open(table_name.to_string(), columns, &self.path);
            self.tables.insert(table_name.to_string(), table);
        }

        Ok(self.tables.get_mut(table_name).unwrap())
    }

    pub fn open(name: &str, mut path: PathBuf) -> Result<Self, PoorlyError> {
        log::info!("Opening database `{}`", name);
        path.push(name);

        if !path.exists() {
            return Err(PoorlyError::DatabaseNotFound(name.to_string()));
        }

        println!("Loading database at {:?}", path);

        let schema = Schema::load(path.as_path());

        log::info!("Database `{}` loaded", name);

        Ok(Self {
            tables: HashMap::new(),
            schema,
            path: path.clone(),
        })
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        if self.path.exists() {
            self.schema.dump(&self.path).expect("Failed to dump schema");
        }
    }
}
