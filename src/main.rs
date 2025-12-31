use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    /// Name to greet.
    #[arg(default_value = "world")]
    name: String,
}

fn main() {
    let cli = Cli::parse();
    println!("{}", cork_core::greeting(&cli.name));
}
