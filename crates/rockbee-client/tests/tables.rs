use rockbee_client::{Result, RockBeeClient};
use rockbee_protocol::{
    Request, Response,
    table::{QueryResult, TableRequest},
    transport::{FrameReader, read_frame, write_frame},
};
use serde_json::json;
use tokio::net::TcpListener;

#[tokio::test]
async fn generated_v7_ids_are_available_before_await_and_put_starts_eagerly() -> Result<()> {
    use std::time::Duration;

    for (ttl, expected_ttl) in [(None, 0), (Some(Duration::from_secs(60)), 60_000)] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?.to_string();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let (input, mut output) = stream.into_split();
            let mut input = FrameReader::new(input);
            let envelope = rockbee_protocol::RequestEnvelope::decode(
                &read_frame(&mut input).await?.ok_or("EOF")?,
            )?;
            let Request::Table(TableRequest::Put {
                table,
                id,
                value,
                ttl_millis,
            }) = envelope.request
            else {
                return Err("expected table put".into());
            };
            assert_eq!(table, "products");
            assert_eq!(uuid::Uuid::parse_str(&id)?.get_version_num(), 7);
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&value)?,
                json!({"name":"Keyboard"})
            );
            assert_eq!(ttl_millis, expected_ttl);
            write_frame(
                &mut output,
                &rockbee_protocol::ResponseEnvelope {
                    request_id: envelope.request_id,
                    response: Response::Ok,
                },
            )
            .await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(id)
        });
        let client: RockBeeClient = RockBeeClient::connect(&address).await?;
        let table = client.table("products");
        let handle = match ttl {
            None => table.put(&json!({"name":"Keyboard"})),
            Some(ttl) => table.put_with_ttl(&json!({"name":"Keyboard"}), ttl),
        };
        let generated = handle.id().to_owned();
        let observed = tokio::time::timeout(Duration::from_secs(2), peer).await???;
        assert_eq!(generated, observed);
        assert!(uuid::Uuid::parse_str(&generated)?.get_timestamp().is_some());
        assert_eq!(handle.await?, generated);
        client.shutdown().await;
    }
    Ok(())
}

#[tokio::test]
async fn table_query_uses_its_name_and_decodes_arbitrary_sql_columns() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let request = rockbee_protocol::RequestEnvelope::decode(
            &read_frame(&mut input).await?.ok_or("EOF")?,
        )?;
        assert_eq!(
            request.request,
            Request::Table(TableRequest::Query {
                table: "products".into(),
                sql: "SELECT count(*) AS n FROM products".into()
            })
        );
        write_frame(
            &mut output,
            &rockbee_protocol::ResponseEnvelope {
                request_id: request.request_id,
                response: Response::Query {
                    result: QueryResult {
                        rows: vec![json!({"n":2})],
                        exported_wal_file_id: 2,
                    },
                },
            },
        )
        .await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });
    let client = RockBeeClient::connect(&address).await?;
    let result = client
        .table("products")
        .query("SELECT count(*) AS n FROM products")
        .await?;
    assert_eq!(result.rows, vec![json!({"n":2})]);
    peer.await??;
    client.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn supplied_ids_and_ttl_survive_json_crud() -> Result<()> {
    use std::time::Duration;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = RockBeeClient::connect(&listener.local_addr()?.to_string()).await?;
    let (stream, _) = listener.accept().await?;
    let peer = tokio::spawn(async move {
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        for (expected, response) in [
            (
                TableRequest::Put {
                    table: "products".into(),
                    id: "sku-1".into(),
                    value: br#"{"price":12}"#.to_vec(),
                    ttl_millis: 1,
                },
                Response::Ok,
            ),
            (
                TableRequest::Get {
                    table: "products".into(),
                    id: "sku-1".into(),
                },
                Response::Value {
                    value: Some(br#"{"price":12}"#.to_vec()),
                },
            ),
            (
                TableRequest::Delete {
                    table: "products".into(),
                    id: "sku-1".into(),
                },
                Response::Ok,
            ),
            (
                TableRequest::Get {
                    table: "products".into(),
                    id: "sku-1".into(),
                },
                Response::Value { value: None },
            ),
        ] {
            let request = rockbee_protocol::RequestEnvelope::decode(
                &read_frame(&mut input).await?.ok_or("EOF")?,
            )?;
            assert_eq!(request.request, Request::Table(expected));
            write_frame(
                &mut output,
                &rockbee_protocol::ResponseEnvelope {
                    request_id: request.request_id,
                    response,
                },
            )
            .await?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });
    let table = client.table("products");
    table
        .put_key("sku-1", &json!({"price":12}), Some(Duration::from_nanos(1)))
        .await?;
    assert_eq!(table.get("sku-1").await?, Some(json!({"price":12})));
    table.delete("sku-1").await?;
    assert_eq!(table.get("sku-1").await?, None);
    peer.await??;
    client.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn invalid_table_inputs_are_not_sent_and_do_not_consume_admission() -> Result<()> {
    use rockbee_client::{ClientConfig, MutationOutcome};
    use std::time::Duration;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = RockBeeClient::connect_with_config(
        &listener.local_addr()?.to_string(),
        ClientConfig::new(Duration::from_secs(2), 1, 1)?,
    )
    .await?;
    let (stream, _) = listener.accept().await?;
    for name in ["", "bad-name", "1products"] {
        let table = client.table(name);
        assert_eq!(
            table
                .put(&json!({}))
                .await
                .expect_err("invalid table")
                .outcome(),
            MutationOutcome::NotSent
        );
        assert!(table.get("id").await.is_err());
        assert_eq!(
            table
                .delete("id")
                .await
                .expect_err("invalid table")
                .outcome(),
            MutationOutcome::NotSent
        );
        assert!(table.query("SELECT 1").await.is_err());
    }
    let table = client.table("products");
    for value in [json!(null), json!([]), json!(true), json!("text"), json!(5)] {
        assert_eq!(
            table
                .put(&value)
                .await
                .expect_err("object required")
                .outcome(),
            MutationOutcome::NotSent
        );
    }
    for id in [String::new(), "x".repeat(1025)] {
        assert_eq!(
            table
                .put_key(&id, &json!({}), None)
                .await
                .expect_err("invalid ID")
                .outcome(),
            MutationOutcome::NotSent
        );
        assert!(table.get(&id).await.is_err());
        assert_eq!(
            table.delete(&id).await.expect_err("invalid ID").outcome(),
            MutationOutcome::NotSent
        );
    }
    for sql in [" ".to_owned(), "SELECT\0".to_owned(), "x".repeat(65537)] {
        assert!(table.query(&sql).await.is_err());
    }
    assert_eq!(
        table
            .put_with_ttl(&json!({}), Duration::MAX)
            .await
            .expect_err("invalid TTL")
            .outcome(),
        MutationOutcome::NotSent
    );
    let peer = tokio::spawn(async move {
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let request = rockbee_protocol::RequestEnvelope::decode(
            &read_frame(&mut input).await?.ok_or("EOF")?,
        )?;
        assert_eq!(
            request.request,
            Request::Table(TableRequest::Get {
                table: "products".into(),
                id: "valid".into()
            })
        );
        write_frame(
            &mut output,
            &rockbee_protocol::ResponseEnvelope {
                request_id: request.request_id,
                response: Response::Value { value: None },
            },
        )
        .await?;
        assert!(read_frame(&mut input).await?.is_none());
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });
    assert_eq!(table.get("valid").await?, None);
    client.shutdown().await;
    peer.await??;
    Ok(())
}

#[tokio::test]
async fn dropped_generated_put_handle_is_drained_by_shutdown() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = RockBeeClient::connect(&listener.local_addr()?.to_string()).await?;
    let (stream, _) = listener.accept().await?;
    let peer = tokio::spawn(async move {
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let request = rockbee_protocol::RequestEnvelope::decode(
            &read_frame(&mut input).await?.ok_or("EOF")?,
        )?;
        assert!(matches!(
            request.request,
            Request::Table(TableRequest::Put { ttl_millis: 10, .. })
        ));
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        write_frame(
            &mut output,
            &rockbee_protocol::ResponseEnvelope {
                request_id: request.request_id,
                response: Response::Ok,
            },
        )
        .await?;
        assert!(read_frame(&mut input).await?.is_none());
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });
    drop(
        client
            .table("products")
            .put_with_ttl(&json!({"price":12}), std::time::Duration::from_millis(10)),
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), client.shutdown()).await?;
    peer.await??;
    Ok(())
}

#[tokio::test]
async fn uncertain_table_mutation_retains_outcome_and_is_not_replayed() -> Result<()> {
    use rockbee_client::MutationOutcome;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = RockBeeClient::connect(&listener.local_addr()?.to_string()).await?;
    let (stream, _) = listener.accept().await?;
    let peer = tokio::spawn(async move {
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        for response in [
            Response::Error {
                message: "durability uncertain".into(),
                mutation_outcome: MutationOutcome::Unknown,
            },
            Response::Value { value: None },
        ] {
            let request = rockbee_protocol::RequestEnvelope::decode(
                &read_frame(&mut input).await?.ok_or("EOF")?,
            )?;
            match &response {
                Response::Error { .. } => assert!(matches!(
                    request.request,
                    Request::Table(TableRequest::Put { .. })
                )),
                _ => assert!(matches!(
                    request.request,
                    Request::Table(TableRequest::Get { .. })
                )),
            }
            write_frame(
                &mut output,
                &rockbee_protocol::ResponseEnvelope {
                    request_id: request.request_id,
                    response,
                },
            )
            .await?;
        }
        assert!(read_frame(&mut input).await?.is_none());
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });
    let table = client.table("products");
    let handle = table.put(&json!({"price":12}));
    let id = handle.id().to_owned();
    assert_eq!(
        handle.await.expect_err("uncertain mutation").outcome(),
        MutationOutcome::Unknown
    );
    assert_eq!(table.get(&id).await?, None);
    client.shutdown().await;
    peer.await??;
    Ok(())
}
