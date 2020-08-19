use crate::{
    config::{DataType, SinkConfig, SinkContext, SinkDescription},
    event::metric::{MetricKind, MetricValue, StatisticKind},
    event::Event,
    sinks::util::{
        tcp::TcpSinkConfig, udp::UdpSinkConfig, unix::UnixSinkConfig, BatchConfig, BatchSettings,
        BatchSink, Buffer, Compression, TowerCompat,
    },
};
use futures::{compat::Future01CompatExt, future::BoxFuture, FutureExt};
use futures01::{stream, Future, Sink};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::task::{Context, Poll};
use tower03::{Service, ServiceBuilder};

#[derive(Debug, Snafu)]
pub enum StatsdError {
    SendError,
    BuildError,
}

pub struct StatsdSvc {
    cx: SinkContext,
    mode: Mode,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct StatsdSinkConfig {
    pub namespace: String,
    #[serde(default)]
    pub batch: BatchConfig,
    #[serde(flatten)]
    pub mode: Mode,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum Mode {
    Tcp(TcpSinkConfig),
    Udp(UdpSinkConfig),
    #[cfg(unix)]
    Unix(UnixSinkConfig),
}

pub fn default_address() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8125)
}

inventory::submit! {
    SinkDescription::new_without_default::<StatsdSinkConfig>("statsd")
}

#[typetag::serde(name = "statsd")]
impl SinkConfig for StatsdSinkConfig {
    fn build(&self, cx: SinkContext) -> crate::Result<(super::RouterSink, super::Healthcheck)> {
        // 1432 bytes is a recommended packet size to fit into MTU
        // https://github.com/statsd/statsd/blob/master/docs/metric_types.md#multi-metric-packets
        // However we need to leave some space for +1 extra trailing event in the buffer.
        // Also one might keep an eye on server side limitations, like
        // mentioned here https://github.com/DataDog/dd-agent/issues/2638
        let batch = BatchSettings::default()
            .bytes(1300)
            .events(1000)
            .timeout(1)
            .parse_config(self.batch.clone())?;
        let namespace = self.namespace.clone();

        let (_sink, healthcheck) = match &self.mode {
            Mode::Tcp(config) => config.build(cx.clone())?,
            Mode::Udp(config) => config.build(cx.clone())?,
            Mode::Unix(config) => config.build(cx.clone())?,
        };
        let statsd = StatsdSvc {
            mode: self.mode.clone(),
            cx: cx.clone(),
        };
        let svc = ServiceBuilder::new().service(statsd);

        let sink = BatchSink::new(
            TowerCompat::new(svc),
            Buffer::new(batch.size, Compression::None),
            batch.timeout,
            cx.acker(),
        )
        .sink_map_err(|_| ())
        .with_flat_map(move |event| stream::once(Ok(encode_event(event, &namespace))));

        Ok((Box::new(sink), healthcheck))
    }

    fn input_type(&self) -> DataType {
        DataType::Metric
    }

    fn sink_type(&self) -> &'static str {
        "statsd"
    }
}

fn push_tag(result: &mut String, name: &str, value: &str) {
    result.push_str(name);
    if value != "true" {
        result.push(':');
        result.push_str(value);
    }
}

fn encode_tags(tags: &BTreeMap<String, String>) -> String {
    let size = tags.iter().map(|(k, v)| k.len() + v.len() + 2).sum();
    let mut result = String::with_capacity(size);
    let mut iter = tags.iter();
    if let Some((name, value)) = iter.next() {
        push_tag(&mut result, name, value);
    }
    for (name, value) in iter {
        result.push(',');
        push_tag(&mut result, name, value);
    }
    result
}

fn encode_event(event: Event, namespace: &str) -> Vec<u8> {
    let mut buf = Vec::new();

    let metric = event.as_metric();
    match metric.kind {
        MetricKind::Incremental => match &metric.value {
            MetricValue::Counter { value } => {
                buf.push(format!("{}:{}", metric.name, value));
                buf.push("c".to_string());
                if let Some(t) = &metric.tags {
                    buf.push(format!("#{}", encode_tags(t)));
                };
            }
            MetricValue::Gauge { value } => {
                buf.push(format!("{}:{:+}", metric.name, value));
                buf.push("g".to_string());
                if let Some(t) = &metric.tags {
                    buf.push(format!("#{}", encode_tags(t)));
                };
            }
            MetricValue::Distribution {
                values,
                sample_rates,
                statistic,
            } => {
                let metric_type = match statistic {
                    StatisticKind::Histogram => "h",
                    StatisticKind::Summary => "d",
                };
                for (val, sample_rate) in values.iter().zip(sample_rates.iter()) {
                    buf.push(format!("{}:{}", metric.name, val));
                    buf.push(metric_type.to_string());
                    if *sample_rate != 1 {
                        buf.push(format!("@{}", 1.0 / f64::from(*sample_rate)));
                    };
                    if let Some(t) = &metric.tags {
                        buf.push(format!("#{}", encode_tags(t)));
                    };
                }
            }
            MetricValue::Set { values } => {
                for val in values {
                    buf.push(format!("{}:{}", metric.name, val));
                    buf.push("s".to_string());
                    if let Some(t) = &metric.tags {
                        buf.push(format!("#{}", encode_tags(t)));
                    };
                }
            }
            _ => {}
        },
        MetricKind::Absolute => {
            if let MetricValue::Gauge { value } = &metric.value {
                buf.push(format!("{}:{}", metric.name, value));
                buf.push("g".to_string());
                if let Some(t) = &metric.tags {
                    buf.push(format!("#{}", encode_tags(t)));
                };
            };
        }
    }

    let mut message: String = buf.join("|");
    if !namespace.is_empty() {
        message = format!("{}.{}", namespace, message);
    };

    let mut body: Vec<u8> = message.into_bytes();
    body.push(b'\n');

    body
}

impl Service<Vec<u8>> for StatsdSvc {
    type Response = ();
    type Error = StatsdError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, frame: Vec<u8>) -> Self::Future {
        let build_result = match &self.mode {
            Mode::Tcp(config) => config.build(self.cx.clone()),
            Mode::Udp(config) => config.build(self.cx.clone()),
            Mode::Unix(config) => config.build(self.cx.clone()),
        };
        let sink = match build_result {
            Ok((sink, _)) => sink,
            Err(_e) => return futures::future::err(StatsdError::BuildError).boxed(),
        };
        sink.send(frame.into())
            .then(|result| result.map(|_sink| ()).map_err(|_| StatsdError::SendError))
            .compat()
            .boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        buffers::Acker,
        event::{metric::MetricKind, metric::MetricValue, metric::StatisticKind, Metric},
        test_util::{collect_n, runtime},
        Event,
    };
    use bytes::Bytes;
    use futures::compat::{Future01CompatExt, Sink01CompatExt};
    use futures::{SinkExt, StreamExt, TryStreamExt};
    use futures01::{sync::mpsc, Sink};
    use tokio_util::codec::BytesCodec;
    #[cfg(feature = "sources-statsd")]
    use {crate::sources::statsd::parser::parse, std::str::from_utf8};

    fn tags() -> BTreeMap<String, String> {
        vec![
            ("normal_tag".to_owned(), "value".to_owned()),
            ("true_tag".to_owned(), "true".to_owned()),
            ("empty_tag".to_owned(), "".to_owned()),
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn test_encode_tags() {
        assert_eq!(
            &encode_tags(&tags()),
            "empty_tag:,normal_tag:value,true_tag"
        );
    }

    #[cfg(feature = "sources-statsd")]
    #[test]
    fn test_encode_counter() {
        let metric1 = Metric {
            name: "counter".to_owned(),
            timestamp: None,
            tags: Some(tags()),
            kind: MetricKind::Incremental,
            value: MetricValue::Counter { value: 1.5 },
        };
        let event = Event::Metric(metric1.clone());
        let frame = &encode_event(event, "");
        let metric2 = parse(from_utf8(&frame).unwrap().trim()).unwrap();
        assert_eq!(metric1, metric2);
    }

    #[cfg(feature = "sources-statsd")]
    #[test]
    fn test_encode_gauge() {
        let metric1 = Metric {
            name: "gauge".to_owned(),
            timestamp: None,
            tags: Some(tags()),
            kind: MetricKind::Incremental,
            value: MetricValue::Gauge { value: -1.5 },
        };
        let event = Event::Metric(metric1.clone());
        let frame = &encode_event(event, "");
        let metric2 = parse(from_utf8(&frame).unwrap().trim()).unwrap();
        assert_eq!(metric1, metric2);
    }

    #[cfg(feature = "sources-statsd")]
    #[test]
    fn test_encode_distribution() {
        let metric1 = Metric {
            name: "distribution".to_owned(),
            timestamp: None,
            tags: Some(tags()),
            kind: MetricKind::Incremental,
            value: MetricValue::Distribution {
                values: vec![1.5],
                sample_rates: vec![1],
                statistic: StatisticKind::Histogram,
            },
        };
        let event = Event::Metric(metric1.clone());
        let frame = &encode_event(event, "");
        let metric2 = parse(from_utf8(&frame).unwrap().trim()).unwrap();
        assert_eq!(metric1, metric2);
    }

    #[cfg(feature = "sources-statsd")]
    #[test]
    fn test_encode_set() {
        let metric1 = Metric {
            name: "set".to_owned(),
            timestamp: None,
            tags: Some(tags()),
            kind: MetricKind::Incremental,
            value: MetricValue::Set {
                values: vec!["abc".to_owned()].into_iter().collect(),
            },
        };
        let event = Event::Metric(metric1.clone());
        let frame = &encode_event(event, "");
        let metric2 = parse(from_utf8(&frame).unwrap().trim()).unwrap();
        assert_eq!(metric1, metric2);
    }
}
