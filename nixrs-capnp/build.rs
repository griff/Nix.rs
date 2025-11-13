extern crate capnpc;

fn main() {
    let capnp = which::which("capnp").unwrap();
    eprintln!("Capnp {}", capnp.display());
    let capnp_include = capnp
        .parent()
        .unwrap()
        .join("../include/capnp")
        .canonicalize()
        .unwrap();
    eprintln!("Capnp include {}", capnp_include.display());
    let path = std::env::current_dir().unwrap();
    capnpc::CompilerCommand::new()
        .import_path(path.join("schema"))
        .crate_provides("capnp_rpc_tokio", [0x8f5d14e1c273738d])
        .src_prefix("schema/nixrs")
        .file("schema/nixrs/nix-daemon.capnp")
        .file("schema/nixrs/nix-types.capnp")
        .file("schema/nixrs/nixrs.capnp")
        .file("schema/nixrs/ip.capnp")
        .file("schema/nixrs/lookup.capnp")
        .default_parent_module(vec!["capnp".into()])
        //.import_path(&capnp_include)
        //.src_prefix(&capnp_include)
        //.file(capnp_include.join("stream.capnp"))
        .run()
        .expect("compiling");
    let path = std::env::current_dir().unwrap();
    eprintln!("The current directory is {}", path.display());
    println!("cargo::rerun-if-changed=schema");
    println!("cargo::rerun-if-changed=build.rs");
}
