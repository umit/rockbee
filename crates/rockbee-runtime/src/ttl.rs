//! Wall-clock TTL validation and filtering shared by both storage roles.

use crate::Result;
use slatedb::KeyValue;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn unix_millis() -> Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "system clock is before the Unix epoch")?;
    i64::try_from(elapsed.as_millis())
        .map_err(|_| "system clock exceeds signed milliseconds".into())
}

pub fn expiry_from(now_millis: i64, ttl_millis: u64) -> Result<Option<i64>> {
    if ttl_millis == 0 {
        return Ok(None);
    }
    let ttl_millis = i64::try_from(ttl_millis).map_err(|_| "TTL exceeds signed milliseconds")?;
    now_millis
        .checked_add(ttl_millis)
        .map(Some)
        .ok_or_else(|| "TTL expiry timestamp overflows signed milliseconds".into())
}

fn is_unexpired(expire_ts: Option<i64>, now_millis: i64) -> bool {
    expire_ts.is_none_or(|expiry| now_millis < expiry)
}

pub fn visible_value(value: Option<KeyValue>) -> Result<Option<Vec<u8>>> {
    let now_millis = unix_millis()?;
    Ok(value
        .and_then(|value| is_unexpired(value.expire_ts, now_millis).then(|| value.value.to_vec())))
}

#[cfg(test)]
mod tests {
    use super::{expiry_from, is_unexpired};

    #[test]
    fn expiry_hides_at_the_deadline_and_forever_has_no_deadline() {
        assert!(is_unexpired(None, i64::MAX));
        assert!(is_unexpired(Some(10), 9));
        assert!(!is_unexpired(Some(10), 10));
        assert!(!is_unexpired(Some(10), 11));
    }

    #[test]
    fn expiry_arithmetic_keeps_zero_forever_and_rejects_overflow() {
        assert_eq!(expiry_from(10, 0).unwrap(), None);
        assert_eq!(expiry_from(10, 5).unwrap(), Some(15));
        assert!(expiry_from(i64::MAX, 1).is_err());
        assert!(expiry_from(0, u64::MAX).is_err());
    }
}
