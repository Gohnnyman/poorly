use clap::Parser;
use env_logger::Env;
use poorly::{
    core::{DatabaseEng, Poorly},
    rest,
};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// A database engine as poor as a house elf
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Path to the server directory
    #[arg(name = "SERVER_FOLDER")]
    server_folder: PathBuf,

    /// Creates a new database called <name>
    #[arg(long = "new", short = 'n', name = "NAME")]
    new_db_name: Option<String>,

    /// Run gRPC server on <port>
    #[arg(long, name = "GRCP_PORT")]
    grpc: Option<u16>,

    /// Run REST server on <port>
    #[arg(long, name = "REST_PORT")]
    rest: Option<u16>,

    /// Use sqlite as the backend
    #[arg(long)]
    sqlite: bool,
}

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let args = Args::parse();

    if args.grpc.is_none() && args.rest.is_none() {
        panic!("No server specified");
    }

    let db = {
        let db = Poorly::open(args.server_folder);
        db.init().unwrap();
        Arc::new(Mutex::new(db)) as Arc<dyn DatabaseEng>
    };

    let rest_server = args
        .rest
        .map(|port| rest::serve(Arc::clone(&db), ([0, 0, 0, 0], port)));

    tokio::select! {
        _ = async { rest_server.unwrap().await }, if rest_server.is_some() => {},
        _ = tokio::signal::ctrl_c() => {
            log::info!(target: "poorly::server", "Shutting down...");
        },
    };
}
