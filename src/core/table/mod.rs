use rusqlite::types::Type;

use super::schema::Columns;
use super::types::{ColumnSet, DataType, PoorlyError, TableMethod, TypedValue};
use super::DatabaseEng;

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: Columns,
    pub nullables: Vec<bool>,
    pub serial: u32,
    pub file: File,
}

#[derive(Debug, Clone)]
struct Row {
    row: ColumnSet,
    offset: u64,
}

// TODO: add cleanup (remove all deleted entries)
impl Table {
    fn next_row(&mut self) -> Option<Result<Row, io::Error>> {
        let mut row = HashMap::new();
        let mut deleted = [0];
        let mut offset;
        loop {
            offset = self.file.stream_position().unwrap();
            self.file.read_exact(&mut deleted).ok()?;

            for (column, data_type) in &self.columns {
                match TypedValue::read(*data_type, &mut self.file) {
                    Ok(value) => row.insert(column.clone(), value),
                    Err(e) => return Some(Err(e)),
                };
            }

            if deleted[0] == 0 {
                break;
            }
        }

        Some(Ok(Row { offset, row }))
    }

    fn delete_at(&mut self, offset: u64) -> Result<(), io::Error> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&[1])?;
        self.file.seek(SeekFrom::Current(-1))?;
        Ok(())
    }

    pub fn open(name: String, columns: Columns, path: &Path) -> Self {
        log::info!("Opening table `{}`", name);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.join(name.clone()))
            .expect("Failed to open table");

        let mut serial = 0u32;

        let mut buf = [0u8; 4];
        let tmp = file.read_exact(&mut buf);
        if let Err(e) = tmp {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                log::debug!("Writing serial `{}` to table `{}`", serial, name);
                file.write_all(serial.to_le_bytes().as_ref())
                    .expect("Failed to write to table");
            } else {
            }
        } else {
            serial = u32::from_le_bytes(buf);
            log::debug!("Read serial `{}` from table `{}`", serial, name)
        }

        let nullables = columns
            .iter()
            .map(|(_, data_type)| {
                if data_type == &DataType::Serial {
                    true
                } else {
                    false
                }
            })
            .collect();

        Self {
            name,
            columns,
            file,
            nullables,
            serial,
        }
    }

    fn check_restrictions(
        &self,
        data_type: DataType,
        table_method: &TableMethod,
    ) -> Result<(), PoorlyError> {
        if table_method == &TableMethod::None {
            return Ok(());
        }

        if data_type == DataType::Serial {
            if table_method == &TableMethod::Insert || table_method == &TableMethod::Update {
                return Err(PoorlyError::InvalidOperation(
                    "Cannot insert to or update serial column".to_string(),
                ));
            }
        }

        Ok(())
    }

    fn check_and_coerce(
        &self,
        mut column_set: ColumnSet,
        table_method: TableMethod,
    ) -> Result<ColumnSet, PoorlyError> {
        log::error!("ABOBA3: {:#?}", column_set);
        let mut coerced = HashMap::new();
        for (column, data_type) in &self.columns {
            if let Some((column, value)) = column_set.remove_entry(column) {
                self.check_restrictions(*data_type, &table_method)?;
                let value = value.coerce(*data_type)?;
                value.validate()?;
                coerced.insert(column, value);
            }
        }
        if column_set.is_empty() {
            Ok(coerced)
        } else {
            Err(PoorlyError::ColumnNotFound(
                column_set.keys().next().unwrap().clone(),
                self.name.clone(),
            ))
        }
    }

    fn check_conditions(
        &self,
        row: &ColumnSet,
        conditions: &ColumnSet,
    ) -> Result<bool, PoorlyError> {
        let mut result = true;
        for (column, value) in conditions {
            if let Some(row_value) = row.get(column) {
                result &= row_value == value;
            } else {
                return Err(PoorlyError::ColumnNotFound(
                    column.clone(),
                    self.name.clone(),
                ));
            }
        }
        Ok(result)
    }

    fn update_serial(&mut self) -> Result<(), PoorlyError> {
        self.file.seek(SeekFrom::Start(0))?;
        self.serial += 1;
        self.file.write_all(&self.serial.to_le_bytes())?;
        self.file.seek(SeekFrom::Start(4))?;
        Ok(())
    }

    pub fn insert(&mut self, values: ColumnSet) -> Result<ColumnSet, PoorlyError> {
        log::error!("ABOBA2: {:#?}", values);
        let values = self.check_and_coerce(values, TableMethod::Insert)?;
        let mut row = vec![0]; // 0 - "not deleted"
        for (name, _type) in &self.columns {
            if _type == &DataType::Serial {
                row.extend_from_slice(&TypedValue::Serial(self.serial).into_bytes());
                continue;
            }

            let value = values
                .get(name)
                .ok_or_else(|| PoorlyError::IncompleteData(name.clone(), self.name.clone()))?;

            row.extend_from_slice(&value.clone().into_bytes());
        }

        self.update_serial()?;

        self.file
            .seek(SeekFrom::End(0))
            .map_err(PoorlyError::IoError)?;
        self.file.write_all(&row).map_err(PoorlyError::IoError)?;
        Ok(values)
    }

    pub fn select(
        &mut self,
        columns: Vec<String>,
        conditions: ColumnSet,
    ) -> Result<Vec<ColumnSet>, PoorlyError> {
        let conditions = self.check_and_coerce(conditions, TableMethod::Select)?;
        let mut selected = Vec::new();
        self.file
            .seek(SeekFrom::Start(4))
            .map_err(PoorlyError::IoError)?;
        while let Some(row) = self.next_row() {
            let Row { mut row, .. } = row.map_err(PoorlyError::IoError)?;

            if !self.check_conditions(&row, &conditions)? {
                continue;
            }

            for column in &columns {
                if !row.contains_key(column) {
                    return Err(PoorlyError::ColumnNotFound(
                        column.clone(),
                        self.name.clone(),
                    ));
                }
            }

            row.retain(|key, _| columns.is_empty() || columns.contains(key));
            selected.push(row);
        }
        Ok(selected)
    }

    pub fn update(
        &mut self,
        set: ColumnSet,
        conditions: ColumnSet,
    ) -> Result<Vec<ColumnSet>, PoorlyError> {
        let set = self.check_and_coerce(set, TableMethod::Update)?;
        let conditions = self.check_and_coerce(conditions, TableMethod::None)?;
        let mut updated = Vec::new();
        let eof = self
            .file
            .seek(SeekFrom::End(0))
            .map_err(PoorlyError::IoError)?;
        self.file
            .seek(SeekFrom::Start(4))
            .map_err(PoorlyError::IoError)?;
        while let Some(row) = self.next_row() {
            let Row { offset, mut row } = row.map_err(PoorlyError::IoError)?;

            if offset == eof {
                break;
            }

            if !self.check_conditions(&row, &conditions)? {
                continue;
            }

            let mut was_updated = false;
            for (column, value) in &set {
                if !row.contains_key(column) {
                    return Err(PoorlyError::ColumnNotFound(
                        column.clone(),
                        self.name.clone(),
                    ));
                }
                let old_value = row.insert(column.clone(), value.clone());
                was_updated |= old_value != Some(value.clone());
            }

            if was_updated {
                updated.push(row.clone());
                self.insert(row)?;
                self.delete_at(offset).map_err(PoorlyError::IoError)?;
            }
        }
        Ok(updated)
    }

    pub fn delete(&mut self, conditions: ColumnSet) -> Result<Vec<ColumnSet>, PoorlyError> {
        let conditions = self.check_and_coerce(conditions, TableMethod::Delete)?;
        let mut deleted = Vec::new();
        self.file
            .seek(SeekFrom::Start(4))
            .map_err(PoorlyError::IoError)?;
        while let Some(row) = self.next_row() {
            let Row { offset, row } = row.map_err(PoorlyError::IoError)?;
            if !self.check_conditions(&row, &conditions)? {
                continue;
            }
            deleted.push(row);
            self.delete_at(offset).map_err(PoorlyError::IoError)?;
        }
        Ok(deleted)
    }

    pub fn drop(&mut self) -> Result<(), PoorlyError> {
        self.file.set_len(0).map_err(PoorlyError::IoError)
    }
}
