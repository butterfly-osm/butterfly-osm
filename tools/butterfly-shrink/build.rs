use protobuf_codegen::Customize;
use std::fs;

fn main() {
    println!("cargo:rerun-if-changed=proto/");
    
    // Create output directory
    fs::create_dir_all("src/proto").expect("Failed to create src/proto directory");
    
    protobuf_codegen::Codegen::new()
        .pure()
        .customize(Customize::default())
        .out_dir("src/proto")
        .inputs(&["proto/fileformat.proto", "proto/osmformat.proto"])
        .include("proto")
        .run()
        .expect("Failed to generate protobuf code");
        
    // Generate mod.rs for the proto module
    fs::write(
        "src/proto/mod.rs",
        "pub mod fileformat;\npub mod osmformat;\n"
    ).expect("Failed to write proto/mod.rs");
}