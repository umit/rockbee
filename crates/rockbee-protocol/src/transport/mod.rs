mod link;

// Shared implementation for external SDK and gateway Upstream; not a routing API.
#[doc(hidden)]
pub use link::{Admission, Link};

use crate::Result;
use crate::{MAX_FRAME, WireMessage, encode_frame};
use futures_util::StreamExt;
use std::io;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

/// Own framing state for the whole connection, including partial reads.
pub struct FrameReader<R> {
    framed: FramedRead<R, LengthDelimitedCodec>,
}
impl<R: AsyncRead + Unpin> FrameReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            framed: LengthDelimitedCodec::builder()
                .max_frame_length(MAX_FRAME - 4)
                .new_read(reader),
        }
    }
    pub async fn read_frame(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.framed
            .next()
            .await
            .transpose()
            .map(|frame| frame.map(|bytes| bytes.to_vec()))
    }
}
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut FrameReader<R>,
) -> io::Result<Option<Vec<u8>>> {
    reader.read_frame().await
}

pub async fn write_frame(
    writer: &mut (impl AsyncWrite + Unpin),
    value: &impl WireMessage,
) -> Result<()> {
    writer.write_all(&encode_frame(value)?).await?;
    writer.flush().await?;
    Ok(())
}
