use clap::Args;
use poorly::{
    core::{
        schema::Columns,
        types::{ColumnSet, DataType, TypedValue},
    },
    grpc::proto,
};
// use poorly::grpc::proto;
use std::{collections::HashMap, error::Error, str::FromStr};
// use structopt::{clap::AppSettings, StructOpt};

#[derive(Debug)]
pub enum Command {
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
    ShowTables {
        db: String,
    },
    Join {
        db: String,
        table1: String,
        table2: String,
        columns: Vec<String>,
        conditions: ColumnSet,
        join_on: HashMap<String, String>,
    },
}

impl FromStr for Command {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.trim().split_whitespace().collect();

        match parts.as_slice() {
            ["Select", db, from, columns, conditions] => {
                // Parse and construct Select variant

                let columns = columns.split(',').map(|s| s.to_string()).collect();
                let conditions = conditions
                    .split(',')
                    .map(|s| parse_key_val::<TypedValue>(s))
                    .collect::<Result<_, _>>()?;

                Ok(Command::Select {
                    db: db.to_string(),
                    from: from.to_string(),
                    columns,
                    conditions,
                })
            }
            ["Insert", db, into, values] => {
                // Parse and construct Insert variant
                let values = values
                    .split(',')
                    .map(|s| parse_key_val::<TypedValue>(s))
                    .collect::<Result<_, _>>()?;
                Ok(Command::Insert {
                    db: db.to_string(),
                    into: into.to_string(),
                    values,
                })
            }
            ["Update", db, table, set, conditions] => {
                // Parse and construct Update variant
                let set = set
                    .split(',')
                    .map(|s| parse_key_val::<TypedValue>(s))
                    .collect::<Result<_, _>>()?;
                let conditions = conditions
                    .split(',')
                    .map(|s| parse_key_val::<TypedValue>(s))
                    .collect::<Result<_, _>>()?;

                Ok(Command::Update {
                    db: db.to_string(),
                    table: table.to_string(),
                    set,
                    conditions,
                })
            }
            ["Delete", db, from, conditions] => {
                // Parse and construct Delete variant
                let conditions = conditions
                    .split(',')
                    .map(|s| parse_key_val::<TypedValue>(s))
                    .collect::<Result<_, _>>()?;

                Ok(Command::Delete {
                    db: db.to_string(),
                    from: from.to_string(),
                    conditions,
                })
            }
            ["Create", db, table, columns] => {
                // Parse and construct Create variant
                let columns = columns
                    .split(',')
                    .map(|s| parse_key_val::<DataType>(s))
                    .collect::<Result<_, _>>()?;

                Ok(Command::Create {
                    db: db.to_string(),
                    table: table.to_string(),
                    columns,
                })
            }
            ["CreateDb", name] => {
                // Parse and construct CreateDb variant
                Ok(Command::CreateDb {
                    name: name.to_string(),
                })
            }
            ["Drop", db, table] => {
                // Parse and construct Drop variant
                Ok(Command::Drop {
                    db: db.to_string(),
                    table: table.to_string(),
                })
            }
            ["DropDb", name] => {
                // Parse and construct DropDb variant
                Ok(Command::DropDb {
                    name: name.to_string(),
                })
            }
            ["Alter", db, table, rename] => {
                // Parse and construct Alter variant
                let rename = rename
                    .split(',')
                    .map(|s| parse_key_val::<String>(s))
                    .collect::<Result<_, _>>()?;

                Ok(Command::Alter {
                    db: db.to_string(),
                    table: table.to_string(),
                    rename,
                })
            }
            ["ShowTables", db] => {
                // Parse and construct ShowTables variant
                Ok(Command::ShowTables { db: db.to_string() })
            }
            ["Join", db, table1, table2, columns, conditions, join_on] => {
                // Parse and construct Join variant
                let columns = columns.split(',').map(|s| s.to_string()).collect();
                let conditions = if conditions != &"_" {
                    conditions
                        .split(',')
                        .map(|s| parse_key_val::<TypedValue>(s))
                        .collect::<Result<_, _>>()?
                } else {
                    HashMap::new()
                };

                let join_on = join_on
                    .split(',')
                    .map(|s| parse_key_val::<String>(s))
                    .collect::<Result<_, _>>()?;

                Ok(Command::Join {
                    db: db.to_string(),
                    table1: table1.to_string(),
                    table2: table2.to_string(),
                    columns,
                    conditions,
                    join_on,
                })
            }
            // Add more patterns for other variants
            _ => Err(anyhow::anyhow!("invalid command: {}", s)),
        }
    }
}

/// Parse a single key-value pair
fn parse_key_val<'a, T>(s: &'a str) -> Result<(String, T), anyhow::Error>
where
    T: TryFrom<&'a str>,
    <T as TryFrom<&'a str>>::Error: Error + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| anyhow::anyhow!("invalid key=value: no `=` found in `{}`", s))?;
    Ok((
        s[..pos].to_string(),
        s[pos + 1..]
            .try_into()
            .map_err(|_| anyhow::anyhow!("cannot convert"))?,
    ))
}

impl From<Command> for proto::Query {
    fn from(command: Command) -> Self {
        macro_rules! parse_key_val {
            ($s:expr) => {
                $s.into_iter().map(|(k, v)| (k, v.into())).collect()
            };
        }

        match command {
            Command::Select {
                db,
                from,
                columns,
                conditions,
            } => proto::Query {
                query: Some(proto::query::Query::Select(proto::Select {
                    db,
                    from,
                    columns,
                    conditions: parse_key_val!(conditions),
                })),
            },
            Command::Insert { db, into, values } => proto::Query {
                query: Some(proto::query::Query::Insert(proto::Insert {
                    db,
                    into,
                    values: parse_key_val!(values),
                })),
            },
            Command::Update {
                db,
                table,
                set,
                conditions,
            } => proto::Query {
                query: Some(proto::query::Query::Update(proto::Update {
                    db,
                    table,
                    set: parse_key_val!(set),
                    conditions: parse_key_val!(conditions),
                })),
            },
            Command::Delete {
                db,
                from,
                conditions,
            } => proto::Query {
                query: Some(proto::query::Query::Delete(proto::Delete {
                    db,
                    from,
                    conditions: parse_key_val!(conditions),
                })),
            },
            Command::Create { db, table, columns } => proto::Query {
                query: Some(proto::query::Query::Create(proto::Create {
                    db,
                    table,
                    columns: parse_key_val!(columns),
                })),
            },
            Command::CreateDb { name } => proto::Query {
                query: Some(proto::query::Query::CreateDb(proto::CreateDb { db: name })),
            },
            Command::Drop { db, table } => proto::Query {
                query: Some(proto::query::Query::Drop(proto::Drop { db, table })),
            },
            Command::DropDb { name } => proto::Query {
                query: Some(proto::query::Query::DropDb(proto::DropDb { db: name })),
            },
            Command::Alter { db, table, rename } => proto::Query {
                query: Some(proto::query::Query::Alter(proto::Alter {
                    db,
                    table,
                    rename,
                })),
            },
            Command::ShowTables { db } => proto::Query {
                query: Some(proto::query::Query::ShowTables(proto::ShowTables { db })),
            },
            Command::Join {
                db,
                table1,
                table2,
                columns,
                conditions,
                join_on,
            } => proto::Query {
                query: Some(proto::query::Query::Join(proto::Join {
                    db,
                    table1,
                    table2,
                    columns,
                    conditions: parse_key_val!(conditions),
                    join_on,
                })),
            },
        }
    }
}
