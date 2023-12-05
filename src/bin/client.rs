use clap::Parser;
use cli::{format::Format, Repl};

mod cli;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// URL of the poorly server
    #[arg(short, long)]
    url: String,

    // The output format
    #[arg(
        short,
        long,
        default_value = "ascii",
        // possible_values = &["ascii", "json", "csv", "html"]
    )]
    format: Format,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut repl = Repl::init(args.url, args.format).await;
    repl.run().await;
}
