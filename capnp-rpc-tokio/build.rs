fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/byte-stream.capnp")
        .run()
        .expect("compiling");
    println!("cargo::rerun-if-changed=schema");
    println!("cargo::rerun-if-changed=build.rs");
}
