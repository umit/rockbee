//! Wire version and field bounds shared by validation and framing.

/// Current wire schema version. Incompatible changes require a new version.
pub const VERSION: u8 = 1;
/// Maximum key size, in bytes.
pub const MAX_KEY_BYTES: usize = 64 * 1024;
/// Maximum value size, in bytes.
pub const MAX_VALUE_BYTES: usize = 1024 * 1024;
/// Maximum ordinary frame payload, excluding the four-byte length header.
pub const MAX_FRAME_PAYLOAD: usize = 2 * 1024 * 1024;
/// Maximum failure message length in UTF-8 bytes, not characters.
pub const MAX_MESSAGE_BYTES: usize = 1024;

/// Maximum remaining service budget accepted by a request, in milliseconds.
pub(crate) const MAX_BUDGET_MS: u64 = 60_000;
