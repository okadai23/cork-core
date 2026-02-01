fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "../../proto/cork/v1/types.proto",
                "../../proto/cork/v1/core.proto",
                "../../proto/cork/v1/worker.proto",
            ],
            &["../../proto"],
        )?;
    Ok(())
}
