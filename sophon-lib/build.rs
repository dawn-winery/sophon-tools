fn main() {
    // if cfg!(feature = "vendored-hpatchz") {
    //     use md5::{Md5, Digest};

    //     println!("cargo::rerun-if-changed=external/hpatchz/hpatchz");

    //     let buf = std::fs::read("external/hpatchz/hpatchz")
    //         .expect("failed to read hpatchz binary");

    //     let mut hash = [0; 16];

    //     let hash = Md5::default()
    //         .chain_update(&buf)
    //         .finalize();

    //     println!("cargo::rustc-env=HPATCHZ_MD5={hash:x}");
    // }

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
