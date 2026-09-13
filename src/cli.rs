use crate::seedlink::DEFAULT_PORT;
use crate::tui::DEFAULT_WINDOW;
use clap::{Parser, Subcommand, ValueEnum};
use std::fmt::Display;
/// Scan for QuakeSaver devices
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    /// The network interface to scan (defaults to primary)
    #[command(subcommand)]
    pub(crate) command: Commands,
}

/// Scan for QuakeSaver devices
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// The network interface to scan (defaults to primary)
    Detect { interface: Option<String> },
    /// Get sensors
    Sensors {
        /// Only list sensors matching these filters (repeat or comma-separate).
        ///
        /// Filters of the same kind widen the selection, filters of different
        /// kinds narrow it: `-f mems,hidra -f online` lists the online MEMS and
        /// HiDRA sensors. Without any filter every sensor is listed.
        #[arg(short, long, value_enum, value_delimiter = ',')]
        filter: Vec<SensorFilter>,
        /// Column to sort the list by
        #[arg(short, long, value_enum, default_value_t = SensorSort::Uid)]
        sort: SensorSort,
        /// Sort in descending order
        #[arg(short, long)]
        reverse: bool,
    },
    /// Download waveform data
    ///
    /// Fetches miniSEED from the SeismiQ FDSN dataselect service, either into a
    /// single file or into an SDS archive directory.
    Waveforms {
        /// Sensor UID to download (repeat or comma-separate)
        #[arg(short = 'u', long = "sensor", value_delimiter = ',')]
        sensors: Vec<String>,
        /// Download every sensor matching these filters, as `sqcli sensors -f`
        /// selects them (repeat or comma-separate)
        #[arg(short, long, value_enum, value_delimiter = ',')]
        filter: Vec<SensorFilter>,
        /// Start of the window: a timestamp (`2026-09-01T12:00:00`), `now`, or
        /// an offset from now (`-1h`)
        #[arg(short = 'S', long)]
        start: Option<String>,
        /// End of the window, in the same spellings as --start [default: now]
        #[arg(short = 'E', long)]
        end: Option<String>,
        /// Length of the window (`10m`, `1h30m`, `2d`); combine with --start or
        /// --end, or use it alone for the window ending now
        #[arg(short, long)]
        duration: Option<String>,
        /// Where to write: a file, `-` for standard output, or a directory (an
        /// existing one, or a path ending in `/`) to fill an SDS archive
        #[arg(short, long)]
        output: String,
        /// Data quality to request
        #[arg(long, value_enum, default_value_t = Quality::B)]
        quality: Quality,
        /// Discard segments shorter than this many seconds
        #[arg(long, default_value_t = 0.0)]
        minimum_length: f64,
        /// Return only the longest segment per stream
        #[arg(long)]
        longest_only: bool,
        /// Split the download into requests of at most this length
        #[arg(long)]
        chunk: Option<String>,
    },
    /// Watch a sensor's waveforms live in the terminal
    ///
    /// Streams through the backend over a websocket, the same route the web
    /// frontend takes. `--seedlink` and `--fdsn` pick a different one. A sensor
    /// named by address on your own network is always read straight from it,
    /// over SeedLink.
    Tui {
        /// The sensor to watch: a UID (`A3B7K9Q2`), or the address of a
        /// sensor on your LAN (`192.168.178.55`), which is always streamed
        /// from the sensor itself
        sensor: String,
        /// Stream over SeedLink rather than the backend websocket
        ///
        /// Uses the backend's relay for a sensor named by UID, and the
        /// sensor's own server for one named by address.
        #[arg(long, group = "transport")]
        seedlink: bool,
        /// Stream by polling the FDSN archive rather than the backend websocket
        ///
        /// Needs nothing of the sensor, but runs as far behind as the archive
        /// does.
        #[arg(long, group = "transport")]
        fdsn: bool,
        /// Seconds of signal to keep on screen
        #[arg(short, long, default_value_t = DEFAULT_WINDOW)]
        window: f64,
        /// The SeedLink port to connect to, with --seedlink
        #[arg(short, long, default_value_t = DEFAULT_PORT)]
        port: u16,
        /// The SeedLink relay to use for sensors named by UID, with --seedlink
        #[arg(long, default_value = "seedlink.network.quakesaver.net")]
        server: String,
    },
    /// Send an action
    Action {
        #[clap(value_enum)]
        action: ActionOptions,
        /// Sensor UId
        sensor_uid: String,
    },
}

/// A selectable filter for the sensor list.
#[derive(Copy, Clone, PartialEq, Eq, Debug, ValueEnum)]
pub enum SensorFilter {
    /// Seen within the last hour
    Online,
    /// Not seen within the last hour
    Offline,
    /// MEMS sensors (ADXL or BMA accelerometer)
    Mems,
    /// HiDRA sensors
    Hidra,
    /// Sensors with an unrecognised hardware revision
    Unknown,
    /// Sensors carrying at least one warning
    Warnings,
}

/// The property a filter selects on.
///
/// Filters sharing a facet are OR-ed together, separate facets are AND-ed, so
/// picking two hardware families widens the list while adding a status narrows
/// it.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Facet {
    Status,
    Family,
    Warnings,
}

/// Every facet a filter can belong to, for walking the AND-ed groups.
pub const FACETS: [Facet; 3] = [Facet::Status, Facet::Family, Facet::Warnings];

impl SensorFilter {
    pub fn facet(self) -> Facet {
        match self {
            SensorFilter::Online | SensorFilter::Offline => Facet::Status,
            SensorFilter::Mems | SensorFilter::Hidra | SensorFilter::Unknown => Facet::Family,
            SensorFilter::Warnings => Facet::Warnings,
        }
    }
}

/// A column the sensor list can be ordered by.
#[derive(Copy, Clone, PartialEq, Eq, Debug, ValueEnum)]
pub enum SensorSort {
    /// Sensor UID
    Uid,
    /// How long ago the sensor was last seen, most recent first
    LastSeen,
    /// When the sensor was first seen, oldest first
    FirstSeen,
    /// Software version
    Version,
    /// Hardware family
    Type,
    /// Number of warnings, fewest first
    Warnings,
}

impl Display for SensorSort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Spelled the way clap accepts it on the command line.
        let key = match self {
            SensorSort::Uid => "uid",
            SensorSort::LastSeen => "last-seen",
            SensorSort::FirstSeen => "first-seen",
            SensorSort::Version => "version",
            SensorSort::Type => "type",
            SensorSort::Warnings => "warnings",
        };
        f.write_str(key)
    }
}

/// The SEED data quality to ask the FDSN service for.
#[derive(Copy, Clone, PartialEq, Eq, Debug, ValueEnum)]
pub enum Quality {
    /// Indeterminate quality
    D,
    /// Raw, as recorded
    R,
    /// Quality controlled
    Q,
    /// Modified, e.g. resampled
    M,
    /// Best available, whatever the archive holds
    B,
}

impl Display for Quality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code = match self {
            Quality::D => "D",
            Quality::R => "R",
            Quality::Q => "Q",
            Quality::M => "M",
            Quality::B => "B",
        };
        f.write_str(code)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum)]
/// An example option
pub enum ActionOptions {
    /// reboot the sensor
    Reboot,
    /// blink the LED
    Blink,
    /// Check for an update
    CheckUpdate,
}

impl Display for ActionOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let action = match self {
            ActionOptions::Reboot => "RebootTrigger",
            ActionOptions::Blink => "LEDPagerTrigger",
            ActionOptions::CheckUpdate => "MenderCheckUpdate",
        };
        f.write_str(action)
    }
}
