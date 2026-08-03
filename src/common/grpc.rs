/// Max gRPC message size for control-plane and transport RPCs.
///
/// Must stay above the in-memory window checkpoint inline limit (8 MiB) so
/// checkpoint/restore payloads are not rejected by tonic's 4 MiB default.
pub const GRPC_MAX_MESSAGE_BYTES: usize = 12 * 1024 * 1024;
