use super::{Result, config::ProbeConfig};
use rockbee_protocol::transport::Link;
use rockbee_protocol::{Request, Response, ServiceRole, StatusSnapshot};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::Instant;

const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

pub(super) async fn run(config: ProbeConfig) -> Result<()> {
    let mut endpoints = Vec::with_capacity(2 + config.readers.len());
    endpoints.push(("gateway", config.gateway));
    endpoints.push(("writer", config.writer));
    endpoints.extend(config.readers.into_iter().map(|reader| ("reader", reader)));

    let deadline = Instant::now() + PROBE_TIMEOUT;
    let mut probes = JoinSet::new();
    for (index, (expected_role, endpoint)) in endpoints.into_iter().enumerate() {
        probes.spawn(async move {
            let record = probe(endpoint, expected_role, deadline).await;
            (index, record)
        });
    }

    let mut records = Vec::new();
    while let Some(result) = probes.join_next().await {
        records.push(result?);
    }
    records.sort_by_key(|(index, _)| *index);
    let mut failed = false;
    for (_, record) in records {
        failed |= !record["ok"].as_bool().unwrap_or(false);
        println!("{}", serde_json::to_string(&record)?);
    }
    if failed {
        Err("one or more endpoints failed Status probe".into())
    } else {
        Ok(())
    }
}

async fn probe(endpoint: String, expected_role: &'static str, deadline: Instant) -> Value {
    let link = Link::new(endpoint.clone(), 1, 0);
    match link.exchange(&Request::Status, deadline).await {
        Ok(Response::Status { snapshot }) => record(endpoint, expected_role, Some(snapshot), None),
        Ok(_) => record(
            endpoint,
            expected_role,
            None,
            Some("response does not match Status request".to_owned()),
        ),
        Err(error) => record(endpoint, expected_role, None, Some(error.to_string())),
    }
}

fn record(
    endpoint: String,
    expected_role: &'static str,
    snapshot: Option<StatusSnapshot>,
    transport_error: Option<String>,
) -> Value {
    let observed_role = snapshot.as_ref().map(|snapshot| role_name(snapshot.role));
    let error = transport_error.or_else(|| {
        (observed_role != Some(expected_role)).then(|| {
            format!(
                "expected {expected_role} role, observed {}",
                observed_role.unwrap_or("unspecified")
            )
        })
    });
    let ok = error.is_none();
    json!({
        "endpoint": endpoint,
        "expected_role": expected_role,
        "observed_role": observed_role,
        "snapshot": snapshot.map(snapshot_json),
        "error": error,
        "ok": ok,
    })
}

fn role_name(role: ServiceRole) -> &'static str {
    match role {
        ServiceRole::Unspecified => "unspecified",
        ServiceRole::Writer => "writer",
        ServiceRole::Reader => "reader",
        ServiceRole::Gateway => "gateway",
    }
}

fn snapshot_json(snapshot: StatusSnapshot) -> Value {
    json!({
        "role": role_name(snapshot.role),
        "connections_current": snapshot.connections_current,
        "connections_accepted_total": snapshot.connections_accepted_total,
        "connections_rejected_total": snapshot.connections_rejected_total,
        "connections_failed_total": snapshot.connections_failed_total,
        "requests_completed_total": snapshot.requests_completed_total,
        "error_responses_total": snapshot.error_responses_total,
    })
}
