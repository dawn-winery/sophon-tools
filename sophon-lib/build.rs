fn main() {
    prost_build::Config::new()
        .compile_protos(
            &[
                "src/protos/download_info.proto",
                "src/protos/update_info.proto",
            ],
            &["src/protos"],
        )
        .expect("failed to compile sophon protobufs");
}
