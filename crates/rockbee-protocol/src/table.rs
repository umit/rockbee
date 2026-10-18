//! Schema-free JSON table requests shared by the SDK, gateway and storage nodes.
use crate::{Result, validate_value_bytes, wire};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MAX_TABLE_NAME: usize = 64;
pub const MAX_ID_BYTES: usize = 1024;
pub const MAX_SQL_BYTES: usize = 64 * 1024;
pub const MAX_QUERY_ROWS: usize = 1000;
pub const MAX_QUERY_RESULT_BYTES: usize = 512 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum TableRequest {
    Put {
        table: String,
        id: String,
        value: Vec<u8>,
        ttl_millis: u64,
    },
    Get {
        table: String,
        id: String,
    },
    Delete {
        table: String,
        id: String,
    },
    Query {
        table: String,
        sql: String,
    },
}

impl TableRequest {
    pub fn is_mutation(&self) -> bool {
        matches!(self, Self::Put { .. } | Self::Delete { .. })
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Put {
                table, id, value, ..
            } => {
                table_key(table, id)?;
                validate_document(value)
            }
            Self::Get { table, id } | Self::Delete { table, id } => {
                table_key(table, id)?;
                Ok(())
            }
            Self::Query { table, sql } => {
                table_prefix(table)?;
                if sql.trim().is_empty() || sql.len() > MAX_SQL_BYTES || sql.contains('\0') {
                    return Err("SQL must contain 1..=65536 bytes without NUL".into());
                }
                Ok(())
            }
        }
    }

    pub(crate) fn message(&self) -> wire::envelope::Message {
        use wire::envelope::Message;
        match self {
            Self::Put {
                table,
                id,
                value,
                ttl_millis,
            } => Message::TablePut(wire::TablePut {
                table: table.clone(),
                id: id.clone(),
                value: value.clone(),
                ttl_millis: *ttl_millis,
            }),
            Self::Get { table, id } => Message::TableGet(wire::TableKey {
                table: table.clone(),
                id: id.clone(),
            }),
            Self::Delete { table, id } => Message::TableDelete(wire::TableKey {
                table: table.clone(),
                id: id.clone(),
            }),
            Self::Query { table, sql } => Message::TableQuery(wire::TableQuery {
                table: table.clone(),
                sql: sql.clone(),
            }),
        }
    }
}

/// Result of a SQL query against a published table projection.
///
/// Rows contain the selected SQL columns and aliases, not necessarily the original
/// document shape. The watermark identifies the projection used by this query;
/// it does not promise that the latest durable document write was included.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryResult {
    /// Result rows as JSON objects, keyed by SQL column names or aliases.
    pub rows: Vec<Value>,
    /// WAL file ID recorded by the published projection; zero when no manifest exists.
    pub exported_wal_file_id: u64,
}

pub(crate) fn query_wire(result: &QueryResult) -> wire::QueryResult {
    wire::QueryResult {
        rows_json: Value::Array(result.rows.clone()).to_string().into_bytes(),
        exported_wal_file_id: result.exported_wal_file_id,
    }
}

pub(crate) fn query_from_wire(result: wire::QueryResult) -> Result<QueryResult> {
    if result.rows_json.len() > MAX_QUERY_RESULT_BYTES {
        return Err("query result exceeds supported bounds".into());
    }
    let rows: Vec<Value> = serde_json::from_slice(&result.rows_json)?;
    if rows.len() > MAX_QUERY_ROWS || rows.iter().any(|row| !row.is_object()) {
        return Err("query rows must be at most 1000 JSON objects".into());
    }
    Ok(QueryResult {
        rows,
        exported_wal_file_id: result.exported_wal_file_id,
    })
}

pub fn validate_document(value: &[u8]) -> Result<()> {
    validate_value_bytes(value)?;
    if !serde_json::from_slice::<Value>(value)?.is_object() {
        return Err("table value must be a JSON object".into());
    }
    Ok(())
}

/// Physical prefix disjoint from all existing raw keys (which begin with zero).
pub fn table_prefix(table: &str) -> Result<Vec<u8>> {
    let bytes = table.as_bytes();
    if bytes.is_empty()
        || bytes.len() > MAX_TABLE_NAME
        || !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        || !bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
    {
        return Err("table name must be an ASCII identifier of 1..=64 bytes".into());
    }
    let mut prefix = Vec::with_capacity(bytes.len() + 2);
    prefix.extend_from_slice(&[1, u8::try_from(bytes.len())?]);
    prefix.extend_from_slice(bytes);
    Ok(prefix)
}

pub fn table_key(table: &str, id: &str) -> Result<Vec<u8>> {
    if id.is_empty() || id.len() > MAX_ID_BYTES {
        return Err("table ID must contain 1..=1024 UTF-8 bytes".into());
    }
    let mut key = table_prefix(table)?;
    key.extend_from_slice(id.as_bytes());
    Ok(key)
}
