use crate::USAGE;
use rockbee_client::Result;
use rockbee_protocol::transport::Link;
use rockbee_protocol::{Request, Response, ServiceRole, StatusSnapshot};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::time::Instant;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

struct Config {
    expected_role: Option<ServiceRole>,
    timeout: Duration,
}

pub(super) async fn run(address: &str, args: &[String]) -> Result<()> {
    let config = Config::parse(args)?;
    let endpoint = address.to_owned();
    let deadline = Instant::now()
        .checked_add(config.timeout)
        .ok_or("status timeout is too large")?;
    let link = Link::new(endpoint.clone(), 1, 0);
    let result = link.exchange(&Request::Status, deadline).await;
    link.disconnect().await;

    let record = match result {
        Ok(Response::Status { snapshot }) => {
            record(&endpoint, config.expected_role, Some(snapshot), None)
        }
        Ok(_) => record(
            &endpoint,
            config.expected_role,
            None,
            Some("response does not match Status request".to_owned()),
        ),
        Err(error) => record(
            &endpoint,
            config.expected_role,
            None,
            Some(error.to_string()),
        ),
    };
    println!("{}", serde_json::to_string(&record)?);

    if record["ok"].as_bool() == Some(true) {
        Ok(())
    } else {
        Err("status request failed".into())
    }
}

impl Config {
    fn parse(args: &[String]) -> Result<Self> {
        let mut expected_role = None;
        let mut timeout = None;
        let mut args = args.iter();
        while let Some(flag) = args.next() {
            let value = args.next().ok_or(USAGE)?;
            match flag.as_str() {
                "--expect-role" if expected_role.is_none() => {
                    expected_role = Some(parse_role(value)?);
                }
                "--timeout-ms" if timeout.is_none() => {
                    timeout = Some(parse_timeout(value)?);
                }
                _ => return Err(USAGE.into()),
            }
        }
        Ok(Self {
            expected_role,
            timeout: timeout.unwrap_or(DEFAULT_TIMEOUT),
        })
    }
}

fn parse_role(value: &str) -> Result<ServiceRole> {
    match value {
        "gateway" => Ok(ServiceRole::Gateway),
        "writer" => Ok(ServiceRole::Writer),
        "reader" => Ok(ServiceRole::Reader),
        _ => Err("--expect-role must be gateway, writer, or reader".into()),
    }
}

fn parse_timeout(value: &str) -> Result<Duration> {
    let timeout_millis = value.parse::<u64>()?;
    if timeout_millis == 0 {
        return Err("--timeout-ms must be greater than zero".into());
    }
    let timeout = Duration::from_millis(timeout_millis);
    if Instant::now().checked_add(timeout).is_none() {
        return Err("--timeout-ms is too large".into());
    }
    Ok(timeout)
}

fn record(
    endpoint: &str,
    expected_role: Option<ServiceRole>,
    snapshot: Option<StatusSnapshot>,
    transport_error: Option<String>,
) -> Value {
    let observed_role = snapshot.as_ref().map(|snapshot| role_name(snapshot.role));
    let expected_role = expected_role.map(role_name);
    let error = transport_error.or_else(|| {
        expected_role
            .filter(|expected_role| Some(*expected_role) != observed_role)
            .map(|expected_role| {
                format!(
                    "expected {expected_role} role, observed {}",
                    observed_role.unwrap_or("unspecified")
                )
            })
    });
    json!({
        "ok": error.is_none(),
        "endpoint": endpoint,
        "expected_role": expected_role,
        "observed_role": observed_role,
        "counters": snapshot.map(snapshot_json),
        "error": error,
        "note": "A Status response proves this endpoint is reachable. It does not prove storage, upstream, or reader freshness health.",
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
        "connections_current": snapshot.connections_current,
        "connections_accepted_total": snapshot.connections_accepted_total,
        "connections_rejected_total": snapshot.connections_rejected_total,
        "connections_failed_total": snapshot.connections_failed_total,
        "requests_completed_total": snapshot.requests_completed_total,
        "error_responses_total": snapshot.error_responses_total,
    })
}
