fn main() {
    #[cfg(feature = "vendored-hpatchz")]
    {
        use base16ct::HexDisplay;
        use md5::{Digest, Md5};

        println!("cargo::rerun-if-changed=external/hpatchz/hpatchz");
        let hpatchz_data =
            std::fs::read("external/hpatchz/hpatchz").expect("Failed to read hpatchz binary");
        let digest = Md5::digest(&hpatchz_data);
        let hpatchz_md5 = HexDisplay(digest.as_ref());
        println!("cargo::rustc-env=HPATCHZ_MD5={hpatchz_md5:x}");
    }
    prost_build::Config::new()
        .message_attribute(
            ".",
            "#[derive(serde::Serialize)] #[serde(rename_all=\"snake_case\")]",
        )
        .compile_protos(
            &[
                "src/protos/SophonManifest.proto",
                "src/protos/SophonPatch.proto",
            ],
            &["src/protos"],
        )
        .unwrap();
}
