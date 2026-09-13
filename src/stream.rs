//! What a live transport hands the view, whichever way the data came in.
//!
//! Three routes carry the same waveforms. The websocket goes through the
//! backend the way the web frontend does and is what `sqcli tui` uses unless
//! told otherwise; SeedLink is the seismological standard and the only route to
//! a sensor on the local network; FDSN polls the archive, which lags but asks
//! nothing of the sensor. They differ only in how the samples arrive, so they
//! all report through the types here.

use chrono::NaiveDateTime;

/// One channel's worth of samples, as a single record or frame carried them.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Chunk {
    /// `NET.STA.LOC.CHAN` where the transport knows it, the channel alone
    /// where it does not; this is what the view keys its panels on.
    pub(crate) stream_id: String,
    pub(crate) channel: String,
    pub(crate) start: NaiveDateTime,
    pub(crate) sample_rate: f64,
    pub(crate) samples: Vec<f64>,
}

/// What a transport tells the view about.
#[derive(Clone, Debug)]
pub(crate) enum Update {
    /// The connection is up; the server named itself.
    Connected {
        server: String,
        station: String,
    },
    Data(Chunk),
    /// Something worth showing that did not end the connection.
    Notice(String),
    /// The connection is gone and the transport is about to try again.
    Disconnected(String),
}

/// Which way the live data comes in.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub(crate) enum Transport {
    /// Through the backend, the way the web frontend does it.
    WebSocket,
    /// Through a SeedLink server: the backend's relay, or a sensor's own.
    SeedLink,
    /// By asking the FDSN archive over and over.
    Fdsn,
}

impl Transport {
    /// How to name this route in the interface and in errors.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Transport::WebSocket => "websocket",
            Transport::SeedLink => "seedlink",
            Transport::Fdsn => "fdsn",
        }
    }

    /// Whether the route goes through the backend, which knows sensors only by
    /// UID and so cannot reach one named by address.
    pub(crate) fn needs_uid(self) -> bool {
        matches!(self, Transport::WebSocket | Transport::Fdsn)
    }
}
