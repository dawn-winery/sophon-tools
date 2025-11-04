fn main() {
    #[cfg(feature = "vendored-hpatchz")]
    {
        use md5::{Digest, Md5};

        println!("cargo::rerun-if-changed=external/hpatchz/hpatchz");
        let hpatchz_data =
            std::fs::read("external/hpatchz/hpatchz").expect("Failed to read hpatchz binary");
        let hpatchz_md5 = Md5::digest(&hpatchz_data);
        println!("cargo::rustc-env=HPATCHZ_MD5={hpatchz_md5:x}");
    }
    protobuf_codegen::Codegen::new()
        .cargo_out_dir("protos")
        .include("src")
        .input("src/protos/SophonManifest.proto")
        .input("src/protos/SophonPatch.proto")
        .run_from_script();
}
