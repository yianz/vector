use super::InternalEvent;
use metrics::counter;

#[derive(Debug)]
pub struct UdpEventReceived {
    pub byte_size: usize,
}

impl InternalEvent for UdpEventReceived {
    fn emit_logs(&self) {
        trace!(message = "received one event.");
    }

    fn emit_metrics(&self) {
        counter!("events_processed", 1,
            "component_kind" => "source",
            "component_type" => "socket",
            "mode" => "udp",
        );
        counter!("bytes_processed", self.byte_size as u64,
            "component_kind" => "source",
            "component_type" => "socket",
            "mode" => "udp",
        );
    }
}

#[derive(Debug)]
pub struct UdpSendFailed {
    pub error: std::io::Error,
}

impl InternalEvent for UdpSendFailed {
    fn emit_logs(&self) {
        error!(message = "error sending datagram.", error = %self.error);
    }

    fn emit_metrics(&self) {
        counter!("send_errors", 1,
            "component_kind" => "source",
            "component_type" => "socket",
            "mode" => "udp",
        );
    }
}

#[derive(Debug)]
pub struct UdpReadFailed {
    pub error: std::io::Error,
}

impl InternalEvent for UdpReadFailed {
    fn emit_logs(&self) {
        error!(message = "error reading datagram.", error = %self.error);
    }

    fn emit_metrics(&self) {
        counter!("read_errors", 1,
            "component_kind" => "source",
            "component_type" => "socket",
            "mode" => "udp",
        );
    }
}
