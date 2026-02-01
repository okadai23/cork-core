fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }
    let include_path = protoc_bin_vendored::include_path()?;
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "../../proto/cork/v1/types.proto",
                "../../proto/cork/v1/core.proto",
                "../../proto/cork/v1/worker.proto",
            ],
            &[
                "../../proto",
                include_path.to_str().ok_or("invalid protoc include path")?,
            ],
        )?;
    Ok(())
}
