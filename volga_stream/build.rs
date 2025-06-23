fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/message_stream.proto")?;
    tonic_build::compile_protos("proto/worker_service.proto")?;
    tonic_build::compile_protos("proto/in_memory_storage_service.proto")?;
    Ok(())
} 