use crate::core::types::{ColumnSet, DataType, PoorlyError, Query};
use crate::core::{database, DatabaseEng};

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use once_cell::sync::Lazy;
use rusqlite::ffi::SQLITE_DBCONFIG_MAINDBNAME;
use warp::http::StatusCode;
use warp::Filter;

impl warp::reject::Reject for PoorlyError {}

static OPENAPI_SPEC: Lazy<serde_json::Value> = Lazy::new(|| {
    let spec = include_str!("../openapi.yaml");
    serde_yaml::from_str(spec).unwrap()
});

impl PoorlyError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            PoorlyError::TableAlreadyExists(_) => StatusCode::CONFLICT,
            PoorlyError::TableNotFound(_) => StatusCode::NOT_FOUND,
            PoorlyError::ColumnAlreadyExists(_, _) => StatusCode::CONFLICT,
            PoorlyError::ColumnNotFound(_, _) => StatusCode::NOT_FOUND,
            PoorlyError::NoColumns => StatusCode::BAD_REQUEST,
            PoorlyError::InvalidName(_) => StatusCode::BAD_REQUEST,
            PoorlyError::InvalidValue(_, _) => StatusCode::BAD_REQUEST,
            PoorlyError::IncompleteData(_, _) => StatusCode::BAD_REQUEST,
            PoorlyError::InvalidDataType(_) => StatusCode::BAD_REQUEST,
            PoorlyError::InvalidOperation(_) => StatusCode::BAD_REQUEST,
            PoorlyError::InvalidEmail => StatusCode::BAD_REQUEST,
            PoorlyError::SqlError(_) => StatusCode::BAD_REQUEST,
            PoorlyError::IoError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            PoorlyError::DatabaseNotFound(_) => StatusCode::NOT_FOUND,
            PoorlyError::DatabaseAlreadyExists(_) => StatusCode::CONFLICT,
            PoorlyError::CannotDropDefaultDb => StatusCode::BAD_REQUEST,
        }
    }
}

pub async fn serve(db_itself: Arc<dyn DatabaseEng>, address: impl Into<SocketAddr>) {
    let database = Arc::clone(&db_itself);
    let select = warp::get()
        .and(warp::path::param())
        .and(warp::path::param())
        .and(warp::path::end())
        .and(warp::query::<ColumnSet>())
        .and_then(move |db: String, from: String, conditions: ColumnSet| {
            let database = Arc::clone(&database);
            execute_on(
                database,
                Query::Select {
                    db,
                    from,
                    conditions,
                    columns: vec![],
                },
            )
        });

    let database = Arc::clone(&db_itself);
    let insert = warp::post()
        .and(warp::path::param())
        .and(warp::path::param())
        .and(warp::path::end())
        .and(warp::body::json())
        .and_then(move |db: String, into: String, values: ColumnSet| {
            let database = Arc::clone(&database);
            execute_on(database, Query::Insert { db, into, values })
        })
        .map(|reply| warp::reply::with_status(reply, StatusCode::CREATED));

    let database = Arc::clone(&db_itself);
    let update = warp::put()
        .and(warp::path::param())
        .and(warp::path::param())
        .and(warp::path::end())
        .and(warp::query::<ColumnSet>())
        .and(warp::body::json())
        .and_then(
            move |db: String, table: String, conditions: ColumnSet, set: ColumnSet| {
                let database = Arc::clone(&database);
                execute_on(
                    database,
                    Query::Update {
                        db,
                        table,
                        conditions,
                        set,
                    },
                )
            },
        );

    let database = Arc::clone(&db_itself);
    let delete = warp::delete()
        .and(warp::path::param())
        .and(warp::path::param())
        .and(warp::path::end())
        .and(warp::query::<ColumnSet>())
        .and_then(move |db: String, from: String, conditions: ColumnSet| {
            let database = Arc::clone(&database);
            execute_on(
                database,
                Query::Delete {
                    db,
                    from,
                    conditions,
                },
            )
        });

    let database = Arc::clone(&db_itself);
    let drop = warp::delete()
        .and(warp::path::param())
        .and(warp::path("drop"))
        .and(warp::path::param())
        .and(warp::path::end())
        .and_then(move |db: String, table: String| {
            let database = Arc::clone(&database);
            execute_on(database, Query::Drop { db, table })
        });

    let database = Arc::clone(&db_itself);
    let create = warp::post()
        .and(warp::path::param())
        .and(warp::path("create"))
        .and(warp::path::param())
        .and(warp::path::end())
        .and(warp::body::json())
        .and_then(
            move |db: String, table: String, columns: HashMap<String, DataType>| {
                let database = Arc::clone(&database);
                let columns = Vec::from_iter(columns.into_iter());
                execute_on(database, Query::Create { db, table, columns })
            },
        )
        .map(|reply| warp::reply::with_status(reply, StatusCode::CREATED));

    let database = Arc::clone(&db_itself);
    let alter = warp::put()
        .and(warp::path::param())
        .and(warp::path("alter"))
        .and(warp::path::param())
        .and(warp::path::end())
        .and(warp::query::<HashMap<String, String>>())
        .and_then(
            move |db: String, table: String, rename: HashMap<String, String>| {
                let database = Arc::clone(&database);
                execute_on(database, Query::Alter { db, table, rename })
            },
        );

    let database = Arc::clone(&db_itself);
    let create_db = warp::post()
        .and(warp::path::param())
        .and(warp::path::end())
        .and_then(move |name: String| {
            let database = Arc::clone(&database);
            execute_on(database, Query::CreateDb { name })
        });

    let database = Arc::clone(&db_itself);
    let drop_db = warp::delete()
        .and(warp::path::param())
        .and(warp::path::end())
        .and_then(move |name: String| {
            let database = Arc::clone(&database);
            execute_on(database, Query::DropDb { name })
        });

    let openapi = warp::get()
        .and(warp::path("openapi.json"))
        .and(warp::path::end())
        .map(|| warp::reply::json(&*OPENAPI_SPEC));

    let index = warp::get()
        .and(warp::path::end())
        .map(|| warp::reply::html(include_str!("../static/index.html")));

    let routes = select
        .or(insert)
        .or(update)
        .or(delete)
        .or(drop)
        .or(create)
        .or(alter)
        .or(create_db)
        .or(drop_db)
        .or(openapi)
        .or(index)
        .with(warp::log("api::rest"))
        .recover(handle_rejection);

    warp::serve(routes).run(address).await;
}

async fn handle_rejection(err: warp::Rejection) -> Result<impl warp::Reply, Infallible> {
    if let Some(error) = err.find::<PoorlyError>() {
        Ok(warp::reply::with_status(
            warp::reply::json(&error),
            error.status_code(),
        ))
    } else {
        Ok(warp::reply::with_status(
            warp::reply::json(&"Invalid request"),
            StatusCode::BAD_REQUEST,
        ))
    }
}

async fn execute_on(
    db: Arc<dyn DatabaseEng>,
    query: Query,
) -> Result<impl warp::Reply, warp::Rejection> {
    let result = db.execute(query)?;
    Ok(warp::reply::json(&result))
}
