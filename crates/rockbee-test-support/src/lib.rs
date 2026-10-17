//! Development-only fixtures; production applications depend directly on their storage role.
pub use rockbee_runtime::{
    DEFAULT_ADDR, Result, RpcConfig, backend, config, memory, protocol, rpc, transport,
};
pub mod storage;
pub use storage::Store;
pub type Server = rockbee_runtime::Server<Store>;
