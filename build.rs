use tower_grpc_build;

fn main() {
    // Build kv
    tower_grpc_build::Config::new()
        .enable_server(true)
        .enable_client(true)
        .build(&["proto/kv/kv.proto"], &["proto/kv"])
        .unwrap_or_else(|e| panic!("protobuf compilation failed: {}", e));
}
