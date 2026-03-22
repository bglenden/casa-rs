fn main() {
    if let Err(error) = casars::run() {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}
