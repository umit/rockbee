#![allow(clippy::unwrap_used, clippy::panic)]

use rockbee_protocol::table::{QueryResult, TableRequest};
use rockbee_protocol::transport::{FrameReader, read_frame, write_frame};
use rockbee_protocol::{Request, RequestEnvelope, Response, ResponseEnvelope};
use serde_json::{Value, json};
use std::process::{Command, Output, Stdio};
use std::time::Duration;
use tokio::{net::TcpListener, time::timeout};

async fn run(address: &str, args: &[&str]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rockbee-client"))
        .args(["--addr", address])
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let result = timeout(Duration::from_secs(4), async {
        loop {
            if child.try_wait().unwrap().is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if result.is_err() {
        child.kill().unwrap();
    }
    let output = child.wait_with_output().unwrap();
    assert!(result.is_ok(), "CLI command timed out");
    output
}

async fn exchange(args: &[&str], response: Response) -> (TableRequest, Value) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let frame = read_frame(&mut input).await.unwrap().unwrap();
        let request = RequestEnvelope::decode(&frame).unwrap();
        write_frame(
            &mut output,
            &ResponseEnvelope {
                request_id: request.request_id,
                response,
            },
        )
        .await
        .unwrap();
        let Request::Table(operation) = request.request else {
            panic!("expected table request")
        };
        operation
    });
    let output = run(&address, args).await;
    if !output.status.success() {
        server.abort();
    }
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    (
        timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap(),
        serde_json::from_slice(&output.stdout).unwrap(),
    )
}

#[tokio::test]
async fn table_put_outputs_generated_id_and_preserves_explicit_id_and_ttl() {
    let (request, output) =
        exchange(&["table-put", "users", r#"{"name":"Ada"}"#], Response::Ok).await;
    let TableRequest::Put {
        table,
        id,
        value,
        ttl_millis,
    } = request
    else {
        panic!("expected put")
    };
    assert_eq!(table, "users");
    assert_eq!(id.len(), 36);
    assert_eq!(output, json!({"id": id}));
    assert_eq!(
        serde_json::from_slice::<Value>(&value).unwrap(),
        json!({"name":"Ada"})
    );
    assert_eq!(ttl_millis, 0);
    let (request, output) = exchange(
        &["table-put", "users", "{}", "--ttl-ms", "25", "--id", "one"],
        Response::Ok,
    )
    .await;
    assert_eq!(
        request,
        TableRequest::Put {
            table: "users".into(),
            id: "one".into(),
            value: b"{}".to_vec(),
            ttl_millis: 25
        }
    );
    assert_eq!(output, json!({"id":"one"}));
}

#[tokio::test]
async fn table_reads_delete_and_query_have_direct_json_output() {
    let (request, output) = exchange(
        &["table-get", "users", "one"],
        Response::Value {
            value: Some(br#"{"name":"Ada"}"#.to_vec()),
        },
    )
    .await;
    assert_eq!(
        request,
        TableRequest::Get {
            table: "users".into(),
            id: "one".into()
        }
    );
    assert_eq!(output, json!({"name":"Ada"}));
    let (_, output) = exchange(
        &["table-get", "users", "missing"],
        Response::Value { value: None },
    )
    .await;
    assert_eq!(output, Value::Null);
    let (request, output) = exchange(&["table-delete", "users", "one"], Response::Ok).await;
    assert_eq!(
        request,
        TableRequest::Delete {
            table: "users".into(),
            id: "one".into()
        }
    );
    assert_eq!(output, json!({"status":"ok"}));
    let sql = "SELECT name FROM users ORDER BY name";
    let result = QueryResult {
        rows: vec![json!({"name":"Ada"})],
        exported_wal_file_id: 19,
    };
    let (request, output) = exchange(
        &["table-query", "users", sql],
        Response::Query {
            result: result.clone(),
        },
    )
    .await;
    assert_eq!(
        request,
        TableRequest::Query {
            table: "users".into(),
            sql: sql.into()
        }
    );
    assert_eq!(output, serde_json::to_value(result).unwrap());
}

#[tokio::test]
async fn invalid_table_arguments_do_not_connect() {
    for args in [
        vec!["table-put", "users", "[]"],
        vec!["table-put", "bad-name", "{}"],
        vec!["table-put", "users", "{}", "--id", ""],
        vec!["table-put", "users", "{}", "--ttl-ms", "bad"],
        vec!["table-put", "users", "{}", "--id", "one", "--id", "two"],
        vec!["table-query", "users", ""],
        vec!["table-get", "users", ""],
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let output = run(&listener.local_addr().unwrap().to_string(), &args).await;
        assert!(!output.status.success(), "accepted {args:?}");
        assert!(
            timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err(),
            "connected for invalid {args:?}"
        );
    }
}
