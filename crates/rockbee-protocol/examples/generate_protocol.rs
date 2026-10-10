// Developer-only generation. Normal builds use checked-in generated modules.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    prost_build::Config::new()
        .out_dir(manifest.join("src/generated"))
        .compile_protos(
            &[manifest.join("../../proto/rockbee.proto")],
            &[manifest.join("../../proto")],
        )?;
    Ok(())
}
