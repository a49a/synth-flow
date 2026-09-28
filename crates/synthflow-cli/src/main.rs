use clap::Parser;

#[derive(Parser)]
#[command(name = "synthflow", version, about = "Local-first synthetic data pipelines")]
struct Cli {}

fn main() {
    Cli::parse();
}
