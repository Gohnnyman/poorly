use proto::database_server::{self as service, DatabaseServer};
use proto::{query, typed_value};
use tonic::{transport::Server, Request, Response, Status};

use crate::core::types::{ColumnSet, PoorlyError, Query, TypedValue};
use crate::core::DatabaseEng;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

#[allow(clippy::derive_partial_eq_without_eq)]
pub mod proto {
    tonic::include_proto!("database");
}

pub struct DatabaseService {
    db: Arc<dyn DatabaseEng>,
}

#[tonic::async_trait]
impl service::Database for DatabaseService {
    async fn execute(
        &self,
        request: Request<proto::Query>,
    ) -> Result<Response<proto::Reply>, Status> {
        let query = request.into_inner();
        let db = Arc::clone(&self.db);
        if let Some(query) = query.query {
            let query = query.into();
            log::info!(target: "api::grpc", "Executing query: {:?}", &query);
            match db.execute(query).await {
                Ok(result) => Ok(Response::new(result.into())),
                Err(err) => Err(err.into()),
            }
        } else {
            Err(Status::invalid_argument("Query is empty"))
        }
    }
}

pub async fn serve(
    db: Arc<dyn DatabaseEng>,
    address: impl Into<SocketAddr>,
) -> Result<(), Box<dyn std::error::Error>> {
    let service = DatabaseService { db };
    let address = address.into();

    log::info!(target: "api::grpc", "Starting gRPC server on {}", address);

    Server::builder()
        .add_service(DatabaseServer::new(service))
        .serve(address)
        .await?;

    Ok(())
}

impl From<PoorlyError> for Status {
    fn from(err: PoorlyError) -> Self {
        match &err {
            PoorlyError::TableNotFound(_) => Status::not_found(err.to_string()),
            PoorlyError::ColumnNotFound(_, _) => Status::not_found(err.to_string()),
            PoorlyError::TableAlreadyExists(_) => Status::already_exists(err.to_string()),
            PoorlyError::ColumnAlreadyExists(_, _) => Status::already_exists(err.to_string()),
            PoorlyError::NoColumns => Status::invalid_argument(err.to_string()),
            PoorlyError::InvalidName(_) => Status::invalid_argument(err.to_string()),
            PoorlyError::InvalidValue(_, _) => Status::invalid_argument(err.to_string()),
            PoorlyError::InvalidDataType(_) => Status::invalid_argument(err.to_string()),
            PoorlyError::IncompleteData(_, _) => Status::invalid_argument(err.to_string()),
            PoorlyError::SqlError(_) => Status::invalid_argument(err.to_string()),
            PoorlyError::IoError(_) => Status::internal(err.to_string()),
            PoorlyError::DatabaseNotFound(_) => Status::not_found(err.to_string()),
            PoorlyError::DatabaseAlreadyExists(_) => Status::already_exists(err.to_string()),
            PoorlyError::InvalidOperation(_) => Status::invalid_argument(err.to_string()),
            PoorlyError::InvalidEmail => Status::invalid_argument(err.to_string()),
            PoorlyError::CannotDropDefaultDb => Status::invalid_argument(err.to_string()),
        }
    }
}

impl From<Vec<ColumnSet>> for proto::Reply {
    fn from(rows: Vec<ColumnSet>) -> Self {
        proto::Reply {
            rows: rows
                .into_iter()
                .map(|row| proto::reply::Row {
                    data: row.into_iter().map(|(k, v)| (k, v.into())).collect(),
                })
                .collect(),
        }
    }
}

impl From<proto::Reply> for Vec<ColumnSet> {
    fn from(reply: proto::Reply) -> Self {
        reply
            .rows
            .into_iter()
            .map(|row| {
                row.data
                    .into_iter()
                    .filter_map(|(k, v)| v.data.map(|v| (k, v.into())))
                    .collect()
            })
            .collect()
    }
}

impl From<proto::query::Query> for Query {
    fn from(query: query::Query) -> Self {
        let convert = |field_set: HashMap<String, proto::TypedValue>| {
            field_set
                .into_iter()
                .filter_map(|(k, v)| v.data.map(|v| (k, v.into())))
                .collect()
        };

        match query {
            query::Query::Select(select) => Query::Select {
                db: select.db,
                from: select.from,
                columns: select.columns,
                conditions: convert(select.conditions),
            },
            query::Query::Insert(insert) => Query::Insert {
                db: insert.db,
                into: insert.into,
                values: convert(insert.values),
            },
            query::Query::Update(update) => Query::Update {
                db: update.db,
                table: update.table,
                set: convert(update.set),
                conditions: convert(update.conditions),
            },
            query::Query::Delete(delete) => Query::Delete {
                db: delete.db,
                from: delete.from,
                conditions: convert(delete.conditions),
            },
            query::Query::Create(create) => Query::Create {
                db: create.db,
                table: create.table,
                columns: create
                    .columns
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
            },
            query::Query::CreateDb(createDb) => Query::CreateDb { name: createDb.db },
            query::Query::Drop(drop) => Query::Drop {
                db: drop.db,
                table: drop.table,
            },
            query::Query::DropDb(dropDb) => Query::DropDb { name: dropDb.db },
            query::Query::Alter(alter) => Query::Alter {
                db: alter.db,
                table: alter.table,
                rename: alter.rename,
            },
            query::Query::ShowTables(show) => Query::ShowTables { db: show.db },
            query::Query::Join(join) => Query::Join {
                db: join.db,
                table1: join.table1,
                table2: join.table2,
                columns: join.columns,
                conditions: convert(join.conditions),
                join_on: join.join_on,
            },
        }
    }
}

impl From<typed_value::Data> for TypedValue {
    fn from(data: typed_value::Data) -> Self {
        match data {
            typed_value::Data::Int(i) => TypedValue::Int(i),
            typed_value::Data::Float(f) => TypedValue::Float(f),
            typed_value::Data::String(s) => TypedValue::String(s),
            typed_value::Data::Serial(u) => TypedValue::Serial(u),
            typed_value::Data::Email(e) => TypedValue::Email(e),
        }
    }
}

impl From<TypedValue> for proto::TypedValue {
    fn from(value: TypedValue) -> Self {
        match value {
            TypedValue::Int(i) => proto::TypedValue {
                data: Some(typed_value::Data::Int(i)),
            },
            TypedValue::Float(f) => proto::TypedValue {
                data: Some(typed_value::Data::Float(f)),
            },
            TypedValue::Char(c) => proto::TypedValue {
                data: Some(typed_value::Data::String(c.to_string())),
            },
            TypedValue::String(s) => proto::TypedValue {
                data: Some(typed_value::Data::String(s)),
            },
            TypedValue::Serial(u) => proto::TypedValue {
                data: Some(typed_value::Data::Serial(u)),
            },
            TypedValue::Email(e) => proto::TypedValue {
                data: Some(typed_value::Data::Email(e)),
            },
        }
    }
}
