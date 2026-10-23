use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::os::fd::AsRawFd;
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::TcpListener;
use tracing::debug;

// the kernel clamps this to net.core.somaxconn
const BACKLOG: i32 = 4096;
// clients send their request right after connecting, so connects that stay silent never wake the server
const DEFER_ACCEPT: Duration = Duration::from_secs(5);

// binds all interfaces, dual stack when ipv6 is available
pub(crate) fn bind(port: u16) -> io::Result<TcpListener> {
    match bind_addr(SocketAddr::from((Ipv6Addr::UNSPECIFIED, port))) {
        Ok(listener) => Ok(listener),
        Err(err) => {
            debug!(error = %err, "ipv6 bind failed, falling back to ipv4");
            bind_addr(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)))
        }
    }
}

fn bind_addr(addr: SocketAddr) -> io::Result<TcpListener> {
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    if addr.is_ipv6() {
        socket.set_only_v6(false)?;
    }
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    set_defer_accept(&socket, DEFER_ACCEPT)?;
    socket.bind(&addr.into())?;
    socket.listen(BACKLOG)?;

    TcpListener::from_std(socket.into())
}

#[expect(unsafe_code, reason = "socket2 has no wrapper for TCP_DEFER_ACCEPT")]
fn set_defer_accept(socket: &Socket, wait: Duration) -> io::Result<()> {
    let secs = libc::c_int::try_from(wait.as_secs()).unwrap_or(libc::c_int::MAX);
    let len = libc::socklen_t::try_from(size_of::<libc::c_int>()).map_err(io::Error::other)?;

    // SAFETY: the fd belongs to `socket`, which outlives the call, and optval points at a live c_int whose size is optlen
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_DEFER_ACCEPT,
            (&raw const secs).cast(),
            len,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn binds_an_ephemeral_port() {
        let listener = bind(0).unwrap();

        assert_ne!(
            listener.local_addr().unwrap().port(),
            0,
            "kernel assigned a port"
        );
    }
}
