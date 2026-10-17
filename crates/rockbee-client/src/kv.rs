//! Default key-value API on `RockBeeClient`.
//!
//! This module is private; its public methods remain available directly on the
//! client. KV and table operations use the same connection and lifecycle.

use crate::{
    MutationError, MutationOutcome, MutationResult, PutHandle, Result, RockBeeClient,
    duration_to_millis, mutation_response, rejected_put,
};
use rockbee_protocol::{Request, Response, validate_put_with_ttl_bytes, validate_request_bytes};
use std::time::Duration;

impl RockBeeClient {
    /// Start a durable PUT immediately and return its eventual completion.
    ///
    /// Calls share the configured in-flight and pending Link admissions with GET and
    /// DELETE. The returned handle may be awaited later; dropping it does not
    /// cancel the registered request or affect other requests.
    /// A timeout after registration leaves the mutation's outcome unknown:
    /// the write may still become durable after the handle returns an error.
    /// A server-side storage error can also follow a persisted write. An error
    /// is not a rollback acknowledgement; mutations are not automatically retried.
    /// `None` and `Some(Duration::ZERO)` store a value without expiry. Positive
    /// sub-millisecond durations round up to one millisecond.
    pub fn put(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> PutHandle {
        if key.is_empty() {
            return rejected_put("key must not be empty".into());
        }
        if value.is_empty() {
            return rejected_put("value must not be empty".into());
        }
        let ttl_millis = match ttl {
            None => None,
            Some(ttl) => match duration_to_millis(ttl) {
                Ok(0) => None,
                Ok(ttl_millis) => Some(ttl_millis),
                Err(error) => return rejected_put(error),
            },
        };
        self.submit_put(key, value, ttl_millis)
    }

    fn submit_put(&self, key: &[u8], value: &[u8], ttl_millis: Option<u64>) -> PutHandle {
        let validation = match ttl_millis {
            Some(ttl_millis) => validate_put_with_ttl_bytes(key, value, ttl_millis),
            None => validate_request_bytes(key, Some(value)),
        };
        if let Err(error) = validation {
            return rejected_put(error);
        }
        self.submit_put_request(match ttl_millis {
            Some(ttl_millis) => Request::PutWithTtl {
                key: key.to_vec(),
                value: value.to_vec(),
                ttl_millis,
            },
            None => Request::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            },
        })
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if key.is_empty() {
            return Err("key must not be empty".into());
        }
        validate_request_bytes(key, None)?;
        let _operation = self.lifecycle.start()?;
        match self.exchange(Request::Get { key: key.to_vec() }).await? {
            Response::Value { value } => Ok(value),
            Response::Error { message, .. } => Err(message.into()),
            _ => Err("unexpected response to get".into()),
        }
    }

    /// Delete a key and wait for its durable acknowledgement.
    ///
    /// A timeout or server-side storage error can leave the deletion's outcome
    /// unknown. An error does not guarantee the value remains present, and the
    /// client does not automatically retry the mutation.
    pub async fn delete(&self, key: &[u8]) -> MutationResult {
        if key.is_empty() {
            return Err(MutationError::new(
                MutationOutcome::NotSent,
                "key must not be empty",
            ));
        }
        validate_request_bytes(key, None)
            .map_err(|error| MutationError::new(MutationOutcome::NotSent, error))?;
        let _operation = self
            .lifecycle
            .start()
            .map_err(|error| MutationError::new(MutationOutcome::NotSent, error))?;
        let response = self
            .exchange(Request::Delete { key: key.to_vec() })
            .await
            .map_err(MutationError::from_error)?;
        mutation_response(response)
    }
}
