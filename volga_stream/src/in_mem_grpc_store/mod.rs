pub mod in_memory_storage_grpc_client;
pub mod in_memory_storage_grpc_server;

pub use in_memory_storage_grpc_client::InMemoryStorageClient;
pub use in_memory_storage_grpc_server::{InMemoryStorageServer, InMemoryStorageServiceImpl};
