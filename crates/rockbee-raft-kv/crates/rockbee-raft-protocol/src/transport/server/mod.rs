//! Bounded typed TCP request servers.
//!
//! A [`Server`] owns socket acceptance and every accepted handler future. It is
//! deliberately the mirror image of [`super::Link`]: `Link` is a client-side
//! connection generation, while this module validates incoming envelopes and
//! calls one typed application handler. The application chooses the request
//! body family through [`RequestHandler`]; the server never knows about Raft,
//! RocksDB, or public KV semantics.
//!
//! For Java readers, `RequestHandler` plays the role of a small interface with
//! one method. `Server::start()` receives one implementation and calls it for
//! each accepted request. The associated `Request` and `Response` types are
//! compile-time contracts, so a gateway handler cannot accidentally receive a
//! private Raft message.

mod config;
mod runtime;

pub use config::{ServerConfig, ServerConfigError};
pub use runtime::{RequestHandler, Server, ServerError};
