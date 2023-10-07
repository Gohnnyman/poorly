use std::collections::HashMap;
use std::fmt;
use std::io;

use rusqlite::types::ToSqlOutput;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::schema::Columns;

pub type ColumnSet = HashMap<String, TypedValue>;

#[derive(Debug, Error)]
pub enum PoorlyError {
    #[error("Table {0} already exists")]
    TableAlreadyExists(String),

    #[error("Table {0} not found")]
    TableNotFound(String),

    #[error("Database {0} not found")]
    DatabaseNotFound(String),

    #[error("Database {0} already exists")]
    DatabaseAlreadyExists(String),

    #[error("Cannot drop default database")]
    CannotDropDefaultDb,

    #[error("Column {0} already exists in table {1}")]
    ColumnAlreadyExists(String, String),

    #[error("Can't create a table without columns")]
    NoColumns,

    #[error("Column {0} not found in table {1}")]
    ColumnNotFound(String, String),

    #[error("Name {0} cannot be used for a table or a column")]
    InvalidName(String),

    #[error("Invalid email format")]
    InvalidEmail,

    #[error("Invalid value {0:?} for datatype {1:?}")]
    InvalidValue(TypedValue, DataType),

    #[error("Incomplete data - missing {0} for table {1}")]
    IncompleteData(String, String),

    #[error("Invalid datatype: {0}")]
    InvalidDataType(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("SQL Error: {0}")]
    SqlError(#[from] rusqlite::Error),
}

impl Serialize for PoorlyError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[derive(Debug, Clone)]
pub enum Query {
    Select {
        db: String,
        from: String,
        columns: Vec<String>,
        conditions: ColumnSet,
    },
    Insert {
        db: String,
        into: String,
        values: ColumnSet,
    },
    Update {
        db: String,
        table: String,
        set: ColumnSet,
        conditions: ColumnSet,
    },
    Delete {
        db: String,
        from: String,
        conditions: ColumnSet,
    },
    Create {
        db: String,
        table: String,
        columns: Columns,
    },
    CreateDb {
        name: String,
    },
    Drop {
        db: String,
        table: String,
    },
    DropDb {
        name: String,
    },
    Alter {
        db: String,
        table: String,
        rename: HashMap<String, String>,
    },
    Join {
        table1: String,
        table2: String,
        columns: Vec<String>,
    },
}

// Used for checking restrictions on columns
// Use None to prevent any checks
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum TableMethod {
    Update,
    Select,
    Insert,
    Delete,
    None,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum TypedValue {
    Int(i64),
    Float(f64),
    Char(char),
    String(String),
    Serial(u32),
    Email(String),
}

#[derive(Copy, Clone, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    Int = 0,
    Float = 1,
    Char = 2,
    String = 3,
    Serial = 4,
    Email = 5,
}

impl rusqlite::ToSql for TypedValue {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, rusqlite::Error> {
        match self {
            TypedValue::Int(i) => i.to_sql(),
            TypedValue::Float(f) => f.to_sql(),
            TypedValue::String(s) => s.to_sql(),
            TypedValue::Char(c) => Ok(ToSqlOutput::from(c.to_string())),
            TypedValue::Serial(u) => Ok(ToSqlOutput::from(u.to_string())),
            TypedValue::Email(e) => e.to_sql(),
        }
    }
}

impl TypedValue {
    pub fn validate(&self) -> Result<(), PoorlyError> {
        match self {
            TypedValue::Email(email) => {
                let email_regex = regex::Regex::new(r"^[\w\-\.]+@([\w-]+\.)+[\w\-]{2,4}$").unwrap();
                if !email_regex.is_match(email) {
                    return Err(PoorlyError::InvalidEmail);
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub fn data_type(&self) -> DataType {
        match self {
            TypedValue::Int(_) => DataType::Int,
            TypedValue::Float(_) => DataType::Float,
            TypedValue::Char(_) => DataType::Char,
            TypedValue::String(_) => DataType::String,
            TypedValue::Serial(_) => DataType::Serial,
            TypedValue::Email(_) => DataType::Email,
        }
    }

    pub fn read<R: io::Read>(data_type: DataType, reader: &mut R) -> Result<Self, io::Error> {
        let mut read_string = || {
            let mut length = [0; 8];
            reader.read_exact(&mut length)?;
            let length = u64::from_le_bytes(length);
            let mut buf = vec![0; length as usize];
            reader.read_exact(&mut buf)?;
            String::from_utf8(buf)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8 string"))
        };

        match data_type {
            DataType::Int => {
                let mut buf = [0; 8];
                reader.read_exact(&mut buf)?;
                Ok(i64::from_le_bytes(buf).into())
            }
            DataType::Float => {
                let mut buf = [0; 8];
                reader.read_exact(&mut buf)?;
                Ok(f64::from_le_bytes(buf).into())
            }
            DataType::Char => {
                let mut buf = [0; 1];
                reader.read_exact(&mut buf)?;
                Ok(char::from(buf[0]).into())
            }
            DataType::String => Ok(TypedValue::String(read_string()?)),
            DataType::Serial => {
                let mut buf = [0; 4];
                reader.read_exact(&mut buf)?;
                Ok(TypedValue::Serial(u32::from_le_bytes(buf)))
            }
            DataType::Email => Ok(TypedValue::Email(read_string()?)),
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        let convert_string = |s: String| {
            let bytes = s.into_bytes();
            let length = (bytes.len() as u64).to_le_bytes().to_vec();
            [length, bytes].concat()
        };

        match self {
            TypedValue::Int(i) => i.to_le_bytes().to_vec(),
            TypedValue::Float(f) => f.to_le_bytes().to_vec(),
            TypedValue::Char(c) => vec![c as u8],
            TypedValue::String(s) => convert_string(s),
            TypedValue::Serial(u) => u.to_le_bytes().to_vec(),
            TypedValue::Email(s) => convert_string(s),
        }
    }

    pub fn coerce(self, to: DataType) -> Result<Self, PoorlyError> {
        let string_to_char = |s: &str| {
            if s.len() == 1 {
                Ok(s.chars().next().unwrap())
            } else {
                Err(PoorlyError::InvalidValue(self.clone(), to))
            }
        };

        if self.data_type() == to {
            return Ok(self);
        }

        match (&self, to) {
            (TypedValue::Int(i), DataType::Float) => Ok(TypedValue::Float(*i as f64)),
            (TypedValue::Int(i), DataType::Serial) => Ok(TypedValue::Serial(*i as u32)),
            (TypedValue::String(s), DataType::Char) => string_to_char(s).map(TypedValue::Char),
            (TypedValue::String(s), DataType::Email) => Ok(TypedValue::Email(s.to_owned())),
            (TypedValue::String(s), DataType::Int) => s
                .parse::<i64>()
                .map(TypedValue::Int)
                .map_err(|_| PoorlyError::InvalidValue(self, to)),
            (TypedValue::String(s), DataType::Float) => s
                .parse::<f64>()
                .map(TypedValue::Float)
                .map_err(|_| PoorlyError::InvalidValue(self, to)),
            (TypedValue::Char(c), DataType::String) => Ok(TypedValue::String(c.to_string())),
            (TypedValue::Char(c), DataType::Int) => c
                .to_string()
                .parse::<i64>()
                .map(TypedValue::Int)
                .map_err(|_| PoorlyError::InvalidValue(self, to)),
            (TypedValue::Char(c), DataType::Float) => c
                .to_string()
                .parse::<f64>()
                .map(TypedValue::Float)
                .map_err(|_| PoorlyError::InvalidValue(self, to)),
            (TypedValue::Email(s), DataType::String) => Ok(TypedValue::String(s.to_owned())),
            (TypedValue::Serial(i), DataType::Int) => Ok(TypedValue::Int(*i as i64)),

            (v, _) => Err(PoorlyError::InvalidValue(v.clone(), to)),
        }
    }
}

impl From<i64> for TypedValue {
    fn from(value: i64) -> Self {
        TypedValue::Int(value)
    }
}

impl From<f64> for TypedValue {
    fn from(value: f64) -> Self {
        TypedValue::Float(value)
    }
}

impl From<u32> for TypedValue {
    fn from(value: u32) -> Self {
        TypedValue::Serial(value)
    }
}

impl From<char> for TypedValue {
    fn from(value: char) -> Self {
        TypedValue::Char(value)
    }
}

impl From<String> for TypedValue {
    fn from(value: String) -> Self {
        TypedValue::String(value)
    }
}

impl From<&str> for TypedValue {
    fn from(value: &str) -> Self {
        TypedValue::String(value.to_string())
    }
}

impl ToString for TypedValue {
    fn to_string(&self) -> String {
        match self {
            TypedValue::Int(i) => i.to_string(),
            TypedValue::Float(f) => f.to_string(),
            TypedValue::Char(c) => c.to_string(),
            TypedValue::String(s) => s.to_string(),
            TypedValue::Serial(u) => u.to_string(),
            TypedValue::Email(e) => e.to_string(),
        }
    }
}

impl fmt::Debug for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            DataType::Int => write!(f, "int"),
            DataType::Float => write!(f, "float"),
            DataType::Char => write!(f, "char"),
            DataType::String => write!(f, "string"),
            DataType::Serial => write!(f, "serial"),
            DataType::Email => write!(f, "email"),
        }
    }
}

impl TryFrom<&str> for DataType {
    type Error = PoorlyError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "int" => Ok(DataType::Int),
            "float" => Ok(DataType::Float),
            "char" => Ok(DataType::Char),
            "string" => Ok(DataType::String),
            "serial" => Ok(DataType::Serial),
            "email" => Ok(DataType::Email),
            _ => Err(PoorlyError::InvalidDataType(s.to_string())),
        }
    }
}

impl From<i32> for DataType {
    fn from(i: i32) -> Self {
        match i {
            0 => DataType::Int,
            1 => DataType::Float,
            2 => DataType::Char,
            3 => DataType::String,
            4 => DataType::Serial,
            5 => DataType::Email,
            _ => unreachable!("Invalid data type"),
        }
    }
}

impl DataType {
    pub fn to_sql(&self) -> String {
        match self {
            DataType::Int => "INTEGER".to_string(),
            DataType::Float => "REAL".to_string(),
            _ => "TEXT".to_string(),
        }
    }
}
