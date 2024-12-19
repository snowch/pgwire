use tokio::io::{self, AsyncRead, AsyncWrite, ReadBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::net::TcpStream;

pub struct LoggingStream {
    inner: TcpStream,
}

impl LoggingStream {
    pub fn new(inner: TcpStream) -> Self {
        Self { inner }
    }
    pub fn into_inner(self) -> TcpStream {
        self.inner
    }
}

impl AsyncRead for LoggingStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let start_filled = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &result {
            let data = &buf.filled()[start_filled..];
            println!("Read data: {:?}", String::from_utf8_lossy(data));
        }
        result
    }
}

impl AsyncWrite for LoggingStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        println!("Write data: {:?}", String::from_utf8_lossy(buf));
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
