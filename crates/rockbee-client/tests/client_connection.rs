use rockbee_client::{Client, Result};
use rockbee_protocol::{RequestEnvelope, Response, ResponseEnvelope, read_frame, write_frame};
use std::io::BufReader;
use std::net::TcpListener;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

#[test]
fn timed_out_connection_cannot_return_a_stale_response() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?.to_string();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut connection = BufReader::new(stream);
        let _ = read_frame(&mut connection).unwrap();
        // Arrives after the client's five-second read timeout.
        thread::sleep(Duration::from_millis(5500));
        let _ = write_frame(
            connection.get_mut(),
            &ResponseEnvelope {
                request_id: 1,
                response: Response::Value {
                    value: Some(b"first value".to_vec()),
                },
            },
        );
    });

    let runtime = tokio::runtime::Runtime::new()?;
    let client = runtime.block_on(Client::connect(&address))?;
    let error = runtime.block_on(client.get(b"first")).unwrap_err();
    let message = error.to_string();
    assert!(message.contains("upstream request timed out"));
    assert!(!message.contains("mutation outcome is unknown"));
    let second = runtime.block_on(client.get(b"second"));
    server.join().unwrap();
    assert!(
        second.is_err(),
        "a failed connection must not serve another request"
    );
    Ok(())
}

#[test]
fn mismatched_response_id_is_rejected_and_retires_the_connection() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?.to_string();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut connection = BufReader::new(stream);
        let frame = read_frame(&mut connection).unwrap().unwrap();
        assert_eq!(RequestEnvelope::decode(&frame).unwrap().request_id, 1);
        write_frame(
            connection.get_mut(),
            &ResponseEnvelope {
                request_id: 2,
                response: Response::Value {
                    value: Some(b"wrong".to_vec()),
                },
            },
        )
        .unwrap();
    });

    let runtime = tokio::runtime::Runtime::new()?;
    let client = runtime.block_on(Client::connect(&address))?;
    assert!(
        runtime.block_on(client.get(b"first")).is_err(),
        "mismatched ID must fail"
    );
    assert!(
        runtime.block_on(client.get(b"second")).is_err(),
        "failed call must retire socket"
    );
    server.join().unwrap();
    Ok(())
}

#[test]
fn wrong_response_kind_is_rejected_and_retires_the_connection() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?.to_string();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut connection = BufReader::new(stream);
        let request =
            RequestEnvelope::decode(&read_frame(&mut connection).unwrap().unwrap()).unwrap();
        write_frame(
            connection.get_mut(),
            &ResponseEnvelope {
                request_id: request.request_id,
                response: Response::Ok,
            },
        )
        .unwrap();
    });

    let runtime = tokio::runtime::Runtime::new()?;
    let client = runtime.block_on(Client::connect(&address))?;
    assert!(
        runtime.block_on(client.get(b"key")).is_err(),
        "GET must reject OK"
    );
    assert!(
        runtime.block_on(client.get(b"next")).is_err(),
        "protocol failure must retire link"
    );
    server.join().unwrap();
    Ok(())
}

#[test]
fn client_preserves_binary_values_and_distinguishes_missing_from_empty() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?.to_string();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut connection = BufReader::new(stream);
        let first =
            RequestEnvelope::decode(&read_frame(&mut connection).unwrap().unwrap()).unwrap();
        assert_eq!(first.request_id, 1);
        assert!(matches!(first.request, rockbee_protocol::Request::Get { key } if key == [0, 255]));
        write_frame(
            connection.get_mut(),
            &ResponseEnvelope {
                request_id: 1,
                response: Response::Value { value: None },
            },
        )
        .unwrap();
        let second =
            RequestEnvelope::decode(&read_frame(&mut connection).unwrap().unwrap()).unwrap();
        assert_eq!(second.request_id, 2);
        write_frame(
            connection.get_mut(),
            &ResponseEnvelope {
                request_id: 2,
                response: Response::Value {
                    value: Some(Vec::new()),
                },
            },
        )
        .unwrap();
    });

    let runtime = tokio::runtime::Runtime::new()?;
    let client = runtime.block_on(Client::connect(&address))?;
    assert_eq!(runtime.block_on(client.get(&[0, 255]))?, None);
    assert_eq!(runtime.block_on(client.get(b"empty"))?, Some(Vec::new()));
    server.join().unwrap();
    Ok(())
}

#[test]
fn cloned_blocking_clients_multiplex_out_of_order_binary_gets() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?.to_string();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut connection = BufReader::new(stream);
        let first =
            RequestEnvelope::decode(&read_frame(&mut connection).unwrap().unwrap()).unwrap();
        let second =
            RequestEnvelope::decode(&read_frame(&mut connection).unwrap().unwrap()).unwrap();
        for request in [second, first] {
            let RequestEnvelope {
                request_id,
                request,
            } = request;
            let rockbee_protocol::Request::Get { key } = request else {
                panic!("expected get");
            };
            write_frame(
                connection.get_mut(),
                &ResponseEnvelope {
                    request_id,
                    response: Response::Value { value: Some(key) },
                },
            )
            .unwrap();
        }
    });

    let runtime = Arc::new(tokio::runtime::Runtime::new()?);
    let client = Arc::new(runtime.block_on(Client::connect(&address))?);
    let start = Arc::new(Barrier::new(3));
    let first_client = Arc::clone(&client);
    let first_start = Arc::clone(&start);
    let first_runtime = Arc::clone(&runtime);
    let first = thread::spawn(move || {
        first_start.wait();
        first_runtime.block_on(first_client.get(&[0, 255]))
    });
    let second_client = Arc::clone(&client);
    let second_start = Arc::clone(&start);
    let second_runtime = Arc::clone(&runtime);
    let second = thread::spawn(move || {
        second_start.wait();
        second_runtime.block_on(second_client.get(&[128, 0]))
    });
    start.wait();
    assert_eq!(first.join().unwrap()?, Some(vec![0, 255]));
    assert_eq!(second.join().unwrap()?, Some(vec![128, 0]));
    server.join().unwrap();
    Ok(())
}
