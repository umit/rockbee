//! JSON table query projection: durable SlateDB changes become immutable Parquet,
//! and reader-side DuckDB executes SQL over those published generations.
pub mod exporter;
mod reader;
mod sql;

pub use exporter::Exporter;
pub use reader::QueryReader;
pub use rockbee_protocol::Result;
pub use rockbee_protocol::table::QueryResult;
