//! Keep user SQL on the selected logical table, never its underlying files.
//!
//! DuckDB parses the SQL itself. We inspect its native tree for data-source
//! capabilities; SQL expressions, joins and aggregates still execute in DuckDB.
use crate::Result;
use duckdb::Connection;
use serde_json::Value;
use std::collections::HashSet;

pub(crate) fn validate(conn: &Connection, table: &str, sql: &str) -> Result<()> {
    let serialized: String = conn.query_row(
        "SELECT CAST(json_serialize_sql(CAST(? AS VARCHAR)) AS VARCHAR)",
        [sql],
        |row| row.get(0),
    )?;
    let tree: Value = serde_json::from_str(&serialized)?;
    let statements = tree["statements"]
        .as_array()
        .ok_or("SQL must be one SELECT")?;
    if tree["error"] != false || statements.len() != 1 {
        return Err("SQL must be one SELECT".into());
    }
    let root = &statements[0]["node"];
    if !matches!(
        root["type"].as_str(),
        Some("SELECT_NODE" | "SET_OPERATION_NODE" | "RECURSIVE_CTE_NODE" | "CTE_NODE")
    ) {
        return Err("SQL must be one SELECT".into());
    }
    visit(root, &HashSet::from([table.to_ascii_lowercase()]))
}

fn visit(value: &Value, visible: &HashSet<String>) -> Result<()> {
    match value {
        Value::Array(values) => {
            for value in values {
                visit(value, visible)?;
            }
        }
        Value::Object(node) => {
            let mut scope = visible.clone();
            // Each query node owns a lexical CTE map. Names from a nested query
            // must never authorize an unrelated base-table reference outside it.
            if let Some(entries) = node
                .get("cte_map")
                .and_then(|map| map.get("map"))
                .and_then(Value::as_array)
            {
                for entry in entries {
                    let name = entry["key"].as_str().ok_or("unsupported CTE tree")?;
                    identifier(name)?;
                    let mut definition_scope = scope.clone();
                    definition_scope.insert(name.to_ascii_lowercase());
                    visit(&entry["value"], &definition_scope)?;
                    scope.insert(name.to_ascii_lowercase());
                }
            }
            if matches!(
                node.get("type").and_then(Value::as_str),
                Some("RECURSIVE_CTE_NODE" | "CTE_NODE")
            ) {
                let name = node
                    .get("cte_name")
                    .and_then(Value::as_str)
                    .ok_or("unsupported recursive CTE tree")?;
                identifier(name)?;
                scope.insert(name.to_ascii_lowercase());
            }
            match node.get("type").and_then(Value::as_str) {
                Some("BASE_TABLE") => {
                    unqualified(value, &["schema_name", "catalog_name"])?;
                    let name = value["table_name"]
                        .as_str()
                        .ok_or("unsupported table tree")?;
                    identifier(name)?;
                    if !scope.contains(&name.to_ascii_lowercase()) {
                        return Err("SQL may reference only the selected table and its CTEs".into());
                    }
                }
                Some("TABLE_FUNCTION") => {
                    let function = &value["function"];
                    function_namespace(function)?;
                    let name = function["function_name"]
                        .as_str()
                        .ok_or("unsupported table function tree")?
                        .to_ascii_lowercase();
                    if !matches!(
                        name.as_str(),
                        "range" | "generate_series" | "unnest" | "json_each" | "json_tree"
                    ) {
                        return Err(
                            "SQL table functions may not open files, catalogs or other queries"
                                .into(),
                        );
                    }
                }
                _ => {}
            }
            if node.get("class").and_then(Value::as_str) == Some("FUNCTION") {
                function_namespace(value)?;
                let name = value["function_name"]
                    .as_str()
                    .ok_or("unsupported SQL function tree")?
                    .to_ascii_lowercase();
                if name.starts_with("read_")
                    || name.starts_with("write_")
                    || name.starts_with("parquet_")
                    || name.starts_with("duckdb_")
                    || name.starts_with("pragma_")
                    || name.starts_with("json_execute")
                    || matches!(
                        name.as_str(),
                        "readfile"
                            | "writefile"
                            | "glob"
                            | "getenv"
                            | "get_env"
                            | "query"
                            | "query_table"
                            | "eval"
                            | "evaluate"
                            | "execute"
                            | "execute_sql"
                            | "load_extension"
                            | "install_extension"
                            | "shell"
                            | "system"
                            | "set_config"
                            | "set_variable"
                            | "setvariable"
                    )
                {
                    return Err(
                        "SQL functions may not access files, environment or executable SQL".into(),
                    );
                }
            }
            for (key, child) in node {
                if key != "cte_map" {
                    visit(child, &scope)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn unqualified(value: &Value, fields: &[&str]) -> Result<()> {
    if fields
        .iter()
        .any(|field| value[*field].as_str().is_some_and(|name| !name.is_empty()))
    {
        return Err("SQL catalog and schema qualification is not supported".into());
    }
    Ok(())
}

fn function_namespace(value: &Value) -> Result<()> {
    unqualified(value, &["catalog"])?;
    // DuckDB itself qualifies desugared array/list constructors with its built-in
    // schema. Qualified functions still pass the exact source/function checks.
    if value["schema"]
        .as_str()
        .is_some_and(|schema| !matches!(schema, "" | "main" | "pg_catalog"))
    {
        return Err("SQL functions must use the built-in namespace".into());
    }
    Ok(())
}

fn identifier(name: &str) -> Result<()> {
    let bytes = name.as_bytes();
    if bytes.is_empty()
        || !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        return Err("SQL tables and CTE names must be plain ASCII identifiers".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_tree_keeps_ctes_subqueries_json_and_safe_relations() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        for sql in [
            "SELECT id, value->'nested'->>'name' AS name FROM products ORDER BY id",
            "WITH x AS (SELECT id FROM products), y AS (SELECT * FROM x) SELECT a.id FROM y a JOIN (SELECT * FROM x) b USING (id)",
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<3) SELECT sum(n) FROM x",
            "SELECT * FROM products UNION ALL SELECT * FROM products",
            "SELECT i FROM range(5) t(i)",
            "SELECT * FROM unnest([1,2,3])",
            "SELECT * FROM json_each('{\"x\":1}')",
            "SELECT '{\"type\":\"BASE_TABLE\",\"table_name\":\"secret\"}' AS literal",
        ] {
            validate(&conn, "products", sql)
                .map_err(|error| format!("valid SQL rejected: {sql}: {error}"))?;
        }
        Ok(())
    }

    #[test]
    fn native_tree_rejects_source_capabilities_in_every_scope() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        for sql in [
            "SELECT * FROM read_parquet('/known/cache')",
            "SELECT * FROM main.read_parquet('/known/cache')",
            "WITH x AS (SELECT * FROM parquet_scan('/known/cache')) SELECT * FROM x",
            "SELECT (SELECT count(*) FROM read_blob('/known/cache'))",
            "SELECT * FROM products UNION ALL SELECT * FROM read_parquet('/known/cache')",
            "SELECT read_text('/known/cache')",
            "SELECT getenv('HOME')",
            "SELECT * FROM query('SELECT * FROM products')",
            "SELECT * FROM query_table('products')",
            "SELECT * FROM information_schema.tables",
            "SELECT * FROM main.products",
            "SELECT * FROM '/known/cache.parquet'",
            "SELECT * FROM x WHERE EXISTS (WITH x AS (SELECT 1) SELECT * FROM x)",
            "SELECT 1; SELECT 2",
            "DROP TABLE products",
        ] {
            assert!(
                validate(&conn, "products", sql).is_err(),
                "source capability accepted: {sql}"
            );
        }
        Ok(())
    }
}
