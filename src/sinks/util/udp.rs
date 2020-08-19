use super::{ByteSink, SinkBuildError};
use crate::{
    config::SinkContext,
    dns::{Resolver, ResolverFuture},
    internal_events::UdpSendFailed,
    sinks::Healthcheck,
};
use bytes::Bytes;
use futures::{FutureExt, TryFutureExt};
use futures01::{future, Async, AsyncSink, Future, Poll, Sink, StartSend};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::time::Duration;
use tokio::time::{delay_for, Delay};
use tokio_retry::strategy::ExponentialBackoff;
use tracing::field;

#[derive(Debug, Snafu)]
pub enum UdpBuildError {
    #[snafu(display("failed to create UDP listener socket, error = {:?}", source))]
    SocketBind { source: io::Error },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UdpSinkConfig {
    pub address: String,
}

impl UdpSinkConfig {
    pub fn new(address: String) -> Self {
        Self { address }
    }

    pub fn build(&self, cx: SinkContext) -> crate::Result<(ByteSink, Healthcheck)> {
        let uri = self.address.parse::<http::Uri>()?;

        let host = uri.host().ok_or(SinkBuildError::MissingHost)?.to_string();
        let port = uri.port_u16().ok_or(SinkBuildError::MissingPort)?;

        let udp = UdpSink::new(host, port, cx.resolver())?;
        let healthcheck = udp_healthcheck();

        Ok((Box::new(udp), healthcheck))
    }
}

pub fn udp_healthcheck() -> Healthcheck {
    Box::new(future::ok(()))
}

pub struct UdpSink {
    host: String,
    port: u16,
    resolver: Resolver,
    state: State,
    span: tracing::Span,
    backoff: ExponentialBackoff,
    socket: UdpSocket,
}

enum State {
    Initializing,
    ResolvingDns(ResolverFuture),
    ResolvedDns(SocketAddr),
    Backoff(Box<dyn Future<Item = (), Error = ()> + Send>),
}

impl UdpSink {
    pub fn new(host: String, port: u16, resolver: Resolver) -> Result<Self, UdpBuildError> {
        let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
        let span = info_span!("connection", %host, %port);
        Ok(Self {
            host,
            port,
            resolver,
            state: State::Initializing,
            span,
            backoff: Self::fresh_backoff(),
            socket: UdpSocket::bind(&from).context(SocketBind)?,
        })
    }

    fn fresh_backoff() -> ExponentialBackoff {
        // TODO: make configurable
        ExponentialBackoff::from_millis(2)
            .factor(250)
            .max_delay(Duration::from_secs(60))
    }

    fn next_delay(&mut self) -> Delay {
        delay_for(self.backoff.next().unwrap())
    }

    fn next_delay01(&mut self) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        let delay = self.next_delay();
        Box::new(async move { Ok(delay.await) }.boxed().compat())
    }

    fn poll_inner(&mut self) -> Result<Async<SocketAddr>, ()> {
        loop {
            self.state = match self.state {
                State::Initializing => {
                    debug!(message = "resolving DNS", host = %self.host);
                    State::ResolvingDns(self.resolver.lookup_ip_01(self.host.clone()))
                }
                State::ResolvingDns(ref mut dns) => match dns.poll() {
                    Ok(Async::Ready(mut addrs)) => match addrs.next() {
                        Some(addr) => {
                            let addr = SocketAddr::new(addr, self.port);
                            debug!(message = "resolved address", %addr);
                            State::ResolvedDns(addr)
                        }
                        None => {
                            error!(message = "DNS resolved no addresses", host = %self.host);
                            State::Backoff(self.next_delay01())
                        }
                    },
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(error) => {
                        error!(message = "unable to resolve DNS", host = %self.host, %error);
                        State::Backoff(self.next_delay01())
                    }
                },
                State::ResolvedDns(addr) => return Ok(Async::Ready(addr)),
                State::Backoff(ref mut delay) => match delay.poll() {
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Ok(Async::Ready(())) => State::Initializing,
                    Err(_) => unreachable!(),
                },
            }
        }
    }
}

impl Sink for UdpSink {
    type SinkItem = Bytes;
    type SinkError = ();

    fn start_send(&mut self, line: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let span = self.span.clone();
        let _enter = span.enter();

        match self.poll_inner() {
            Ok(Async::Ready(address)) => {
                debug!(
                    message = "sending event.",
                    bytes = &field::display(line.len())
                );
                if let Err(error) = self.socket.send_to(&line, address) {
                    emit!(UdpSendFailed { error });
                }
                Ok(AsyncSink::Ready)
            }
            Ok(Async::NotReady) => Ok(AsyncSink::NotReady(line)),
            Err(_) => unreachable!(),
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }
}
