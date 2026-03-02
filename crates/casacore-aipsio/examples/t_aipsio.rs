fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = casacore_aipsio::demo::run_taipsio_like_demo()?;
    print!("{output}");
    Ok(())
}
