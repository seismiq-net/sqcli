/// openapi-generator generate -i https://api.network.quakesaver.net/api/v1/openapi.json  -g rust -o ./sensor-api
use chrono::prelude::*;
use chrono::{Duration, NaiveDateTime, TimeDelta};

use crate::cli::{SensorFilter, SensorSort, FACETS};
use crate::table::print_table;
use log::{debug, info, warn};
use sensor_api::apis::configuration::Configuration;
use sensor_api::apis::sensors_api::{trigger_sensor_action, TriggerSensorActionError};
use sensor_api::apis::users_api::{get_access_token_by_login, get_sensors, GetSensorsError};
use sensor_api::apis::Error;
use sensor_api::models::Sensor;
use serde_json::Value;
use std::fmt;
use std::str::FromStr;

const BASE_URL: &str = "https://api.network.quakesaver.net";
const OFFLINE_THRESHOLD: TimeDelta = Duration::hours(1);

pub(crate) struct StateDisconnected {}

pub(crate) struct StateConnected {
    token: String,
}

pub(crate) struct SMIQClient<S> {
    state: S,
}

impl SMIQClient<StateDisconnected> {
    pub(crate) fn new() -> Self {
        SMIQClient::<StateDisconnected> {
            state: StateDisconnected {},
        }
    }

    pub(crate) async fn authenticate(self) -> SMIQClient<StateConnected> {
        let _token = get_auth_token().await.expect("Authentication failed");
        let state = StateConnected { token: _token };
        SMIQClient::<StateConnected> { state }
    }
}

impl SMIQClient<StateConnected> {
    pub(crate) fn get_token(&self) -> &str {
        &self.state.token
    }

    /// The generated client's configuration, carrying our access token.
    pub(crate) fn api_configuration(&self) -> Configuration {
        Configuration {
            base_path: BASE_URL.to_string(),
            oauth_access_token: Some(self.get_token().to_string()),
            ..Default::default()
        }
    }
}

struct PrettyDuration(Duration);

impl fmt::Display for PrettyDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total_seconds = self.0.num_seconds();
        let seconds = total_seconds % 60;
        let minutes = (total_seconds / 60) % 60;
        let hours = (total_seconds / 3600) % 24;
        let days = total_seconds / 86400;

        if days > 9 {
            write!(f, "{}d", days)
        } else if days > 0 {
            write!(f, "{}d {}h", days, hours)
        } else if hours > 0 {
            write!(f, "{}h {}m", hours, minutes)
        } else if minutes > 0 {
            write!(f, "{}m {}s", minutes, seconds)
        } else {
            write!(f, "{}s", seconds)
        }
    }
}

/// The hardware family of a sensor, as far as the CLI cares.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum SensorKind {
    Mems,
    Hidra,
    Unknown,
}

impl SensorKind {
    /// Classify by the accelerometer named in the hardware revision (e.g.
    /// `OPI0_ADXL_1.0`, `RPI4_HIDRA_1.0`), so new revisions of a board we
    /// already know keep being recognised.
    fn from_revision(revision: &str) -> Self {
        if revision.contains("HIDRA") {
            SensorKind::Hidra
        } else if revision.contains("ADXL") || revision.contains("BMA") {
            SensorKind::Mems
        } else {
            SensorKind::Unknown
        }
    }
}

impl fmt::Display for SensorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            SensorKind::Mems => "MEMS",
            SensorKind::Hidra => "HiDRA",
            SensorKind::Unknown => "unknown",
        };
        f.write_str(name)
    }
}

/// The icon for a hardware revision, padded to two terminal columns so the
/// single-width symbols line up with the emoji.
fn revision_icon(revision: &str) -> &'static str {
    if revision.contains("HIDRA") {
        "🌀"
    } else if revision.contains("ADXL") {
        "▣ "
    } else if revision.contains("BMA") {
        "🌋"
    } else {
        "? "
    }
}

/// A sensor prepared for display: everything the filters, the sort and the
/// table need, parsed once.
pub(crate) struct SensorRow {
    pub(crate) uid: String,
    kind: SensorKind,
    revision: String,
    software_version: String,
    first_seen: Option<NaiveDateTime>,
    last_seen: Duration,
    warnings: usize,
}

impl SensorRow {
    /// Returns `None` for a sensor whose `last_updated` the API sent in a shape
    /// we cannot read, since every filter and sort key hangs off it.
    fn from_sensor(sensor: &Sensor, now: NaiveDateTime) -> Option<Self> {
        let last_updated = match NaiveDateTime::from_str(&sensor.last_updated) {
            Ok(timestamp) => timestamp,
            Err(e) => {
                warn!(
                    "skipping sensor {}: cannot parse last_updated {:?} ({})",
                    sensor.uid, sensor.last_updated, e
                );
                return None;
            }
        };
        Some(SensorRow {
            uid: sensor.uid.clone(),
            kind: SensorKind::from_revision(&sensor.hardware_revision),
            revision: sensor.hardware_revision.clone(),
            software_version: sensor.software_version.clone(),
            first_seen: NaiveDateTime::from_str(&sensor.first_seen).ok(),
            last_seen: now - last_updated,
            warnings: sensor
                .warnings
                .as_ref()
                .and_then(|warnings| warnings.data.as_ref())
                .map_or(0, |data| data.len()),
        })
    }

    fn online(&self) -> bool {
        self.last_seen <= OFFLINE_THRESHOLD
    }

    fn matches(&self, filter: SensorFilter) -> bool {
        match filter {
            SensorFilter::Online => self.online(),
            SensorFilter::Offline => !self.online(),
            SensorFilter::Mems => self.kind == SensorKind::Mems,
            SensorFilter::Hidra => self.kind == SensorKind::Hidra,
            SensorFilter::Unknown => self.kind == SensorKind::Unknown,
            SensorFilter::Warnings => self.warnings > 0,
        }
    }

    /// A sensor is selected when it satisfies every facet the user filtered on.
    /// Facets nobody filtered on let everything through.
    fn selected_by(&self, filters: &[SensorFilter]) -> bool {
        FACETS.iter().all(|facet| {
            let mut chosen = filters.iter().filter(|f| f.facet() == *facet).peekable();
            chosen.peek().is_none() || chosen.any(|f| self.matches(*f))
        })
    }
}

fn sort_sensors(rows: &mut [SensorRow], sort: SensorSort, reverse: bool) {
    // The UID tie-break keeps the order stable for sensors that share a key.
    match sort {
        SensorSort::Uid => rows.sort_by(|a, b| a.uid.cmp(&b.uid)),
        SensorSort::LastSeen => rows.sort_by(|a, b| {
            a.last_seen
                .cmp(&b.last_seen)
                .then_with(|| a.uid.cmp(&b.uid))
        }),
        // Sensors with an unreadable `first_seen` sort to the front.
        SensorSort::FirstSeen => rows.sort_by(|a, b| {
            a.first_seen
                .cmp(&b.first_seen)
                .then_with(|| a.uid.cmp(&b.uid))
        }),
        SensorSort::Version => rows.sort_by(|a, b| {
            a.software_version
                .cmp(&b.software_version)
                .then_with(|| a.uid.cmp(&b.uid))
        }),
        SensorSort::Type => rows.sort_by(|a, b| {
            a.kind
                .to_string()
                .cmp(&b.kind.to_string())
                .then_with(|| a.revision.cmp(&b.revision))
                .then_with(|| a.uid.cmp(&b.uid))
        }),
        SensorSort::Warnings => {
            rows.sort_by(|a, b| a.warnings.cmp(&b.warnings).then_with(|| a.uid.cmp(&b.uid)))
        }
    }
    if reverse {
        rows.reverse();
    }
}

/// Fetch the account's sensors and keep the ones the filters select.
///
/// Shared by `sensors`, which prints them, and `waveforms`, which downloads
/// them, so both understand `--filter` the same way.
pub(crate) async fn selected_sensors(
    client: &SMIQClient<StateConnected>,
    filters: &[SensorFilter],
) -> Result<Vec<SensorRow>, Error<GetSensorsError>> {
    let response = get_sensors(&client.api_configuration(), None, Some(1000), None).await?;
    let sensors: Vec<Sensor> = response.sensors.into_values().collect();
    if sensors.len() == 1000 {
        warn!("hit sensor request limit");
    }

    let now = Local::now().naive_utc();
    Ok(sensors
        .iter()
        .filter_map(|sensor| SensorRow::from_sensor(sensor, now))
        .filter(|row| row.selected_by(filters))
        .collect())
}

pub(crate) async fn print_sensors(
    filters: &[SensorFilter],
    sort: SensorSort,
    reverse: bool,
) -> Result<(), Error<GetSensorsError>> {
    let client = SMIQClient::new().authenticate().await;
    let mut rows = selected_sensors(&client, filters).await?;
    sort_sensors(&mut rows, sort, reverse);

    present_sensors(&rows);
    Ok(())
}

fn present_sensors(rows: &[SensorRow]) {
    if rows.is_empty() {
        println!("no sensors match the selected filters");
        return;
    }

    let cells: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            vec![
                revision_icon(&row.revision).to_string(),
                row.uid.clone(),
                row.kind.to_string(),
                if row.online() { "online" } else { "offline" }.to_string(),
                row.software_version.clone(),
                PrettyDuration(row.last_seen).to_string(),
                match row.warnings {
                    0 => String::new(),
                    count => count.to_string(),
                },
            ]
        })
        .collect();

    print_table(
        &["", "UID", "TYPE", "STATUS", "VERSION", "LAST SEEN", "WARN"],
        &cells,
    );
}

/// The account to log in with, from the environment or a local `.env`.
///
/// Separate from the login itself so a command can find out whether it has
/// credentials before it takes over the screen.
pub(crate) fn credentials() -> Result<(String, String), String> {
    if let Err(e) = dotenvy::dotenv() {
        debug!("Failed to read .env file. Error: {}", e);
    }
    let read = |name: &str| {
        std::env::var(name)
            .map_err(|_| format!("{} is not set; see the README for how to sign in", name))
    };
    Ok((read("SEISMIQ_USERNAME")?, read("SEISMIQ_PASSWORD")?))
}

async fn get_auth_token() -> Result<String, Box<dyn std::error::Error>> {
    let (username, password) = credentials()?;
    let configuration = Configuration {
        base_path: BASE_URL.to_string(),
        ..Default::default()
    };
    let token =
        get_access_token_by_login(&configuration, &username, &password, None, None, None, None)
            .await?;
    Ok(token.access_token)
}

pub(crate) async fn trigger_action(
    action_name: &str,
    sensor_uid: &str,
) -> Result<(), Error<TriggerSensorActionError>> {
    let empty_body: Value = serde_json::from_str("{}")?;
    info!("triggering action {} on sensor {}", action_name, sensor_uid);
    let client = SMIQClient::new();
    let connected_client = client.authenticate().await;
    let response = trigger_sensor_action(
        &connected_client.api_configuration(),
        sensor_uid,
        action_name,
        empty_body,
    )
    .await?;
    info!("{}", response["info"]);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A row with sensible defaults, so each test only states what it cares about.
    fn row(uid: &str, revision: &str, minutes_since_seen: i64) -> SensorRow {
        SensorRow {
            uid: uid.to_string(),
            kind: SensorKind::from_revision(revision),
            revision: revision.to_string(),
            software_version: "1.0.0".to_string(),
            first_seen: None,
            last_seen: Duration::minutes(minutes_since_seen),
            warnings: 0,
        }
    }

    fn uids(rows: &[SensorRow]) -> Vec<&str> {
        rows.iter().map(|r| r.uid.as_str()).collect()
    }

    #[test]
    fn revisions_map_to_hardware_families() {
        assert_eq!(SensorKind::from_revision("OPI0_ADXL_1.0"), SensorKind::Mems);
        assert_eq!(SensorKind::from_revision("RPI0_BMA_0.6"), SensorKind::Mems);
        assert_eq!(
            SensorKind::from_revision("RPI4_HIDRA_1.0"),
            SensorKind::Hidra
        );
        // A future revision of a board we know still lands in its family.
        assert_eq!(SensorKind::from_revision("OPI0_ADXL_1.1"), SensorKind::Mems);
        assert_eq!(SensorKind::from_revision("unknown"), SensorKind::Unknown);
    }

    #[test]
    fn online_is_decided_by_the_offline_threshold() {
        assert!(row("A", "OPI0_ADXL_1.0", 30).online());
        assert!(!row("B", "OPI0_ADXL_1.0", 90).online());
    }

    #[test]
    fn no_filters_selects_everything() {
        assert!(row("A", "OPI0_ADXL_1.0", 30).selected_by(&[]));
        assert!(row("B", "RPI4_HIDRA_1.0", 900).selected_by(&[]));
    }

    #[test]
    fn filters_in_one_facet_widen_the_selection() {
        let families = [SensorFilter::Mems, SensorFilter::Hidra];
        assert!(row("A", "OPI0_ADXL_1.0", 30).selected_by(&families));
        assert!(row("B", "RPI4_HIDRA_1.0", 30).selected_by(&families));
        assert!(!row("C", "unknown", 30).selected_by(&families));
    }

    #[test]
    fn filters_across_facets_narrow_the_selection() {
        let online_mems = [SensorFilter::Online, SensorFilter::Mems];
        assert!(row("A", "OPI0_ADXL_1.0", 30).selected_by(&online_mems));
        // Right family, but stale.
        assert!(!row("B", "OPI0_ADXL_1.0", 90).selected_by(&online_mems));
        // Online, but wrong family.
        assert!(!row("C", "RPI4_HIDRA_1.0", 30).selected_by(&online_mems));
    }

    #[test]
    fn the_warnings_filter_is_its_own_facet() {
        let mut warned = row("A", "OPI0_ADXL_1.0", 30);
        warned.warnings = 2;
        let quiet = row("B", "OPI0_ADXL_1.0", 30);

        assert!(warned.selected_by(&[SensorFilter::Warnings]));
        assert!(!quiet.selected_by(&[SensorFilter::Warnings]));
        // And it still ANDs with a family filter.
        assert!(!warned.selected_by(&[SensorFilter::Warnings, SensorFilter::Hidra]));
    }

    #[test]
    fn sorting_by_uid_is_alphabetical_and_reversible() {
        let mut rows = vec![
            row("C", "OPI0_ADXL_1.0", 10),
            row("A", "OPI0_ADXL_1.0", 20),
            row("B", "OPI0_ADXL_1.0", 30),
        ];

        sort_sensors(&mut rows, SensorSort::Uid, false);
        assert_eq!(uids(&rows), ["A", "B", "C"]);

        sort_sensors(&mut rows, SensorSort::Uid, true);
        assert_eq!(uids(&rows), ["C", "B", "A"]);
    }

    #[test]
    fn sorting_by_last_seen_puts_the_freshest_first() {
        let mut rows = vec![
            row("A", "OPI0_ADXL_1.0", 600),
            row("B", "OPI0_ADXL_1.0", 5),
            row("C", "OPI0_ADXL_1.0", 60),
        ];

        sort_sensors(&mut rows, SensorSort::LastSeen, false);
        assert_eq!(uids(&rows), ["B", "C", "A"]);
    }

    #[test]
    fn rows_sharing_a_sort_key_fall_back_to_the_uid() {
        let mut rows = vec![row("B", "OPI0_ADXL_1.0", 10), row("A", "RPI0_BMA_0.6", 10)];

        sort_sensors(&mut rows, SensorSort::LastSeen, false);
        assert_eq!(uids(&rows), ["A", "B"]);
    }

    #[test]
    fn a_sensor_with_an_unreadable_last_updated_is_dropped() {
        let now = NaiveDateTime::from_str("2026-08-16T12:00:00").unwrap();
        let mut sensor = Sensor {
            uid: "A".to_string(),
            software_version: "1.0.0".to_string(),
            hardware_revision: "OPI0_ADXL_1.0".to_string(),
            first_seen: "not a timestamp".to_string(),
            last_updated: "2026-08-16T11:30:00".to_string(),
            warnings: None,
            max_data_product_count: 0,
        };

        // An unreadable `first_seen` only costs us the sort key.
        let parsed = SensorRow::from_sensor(&sensor, now).expect("row");
        assert_eq!(parsed.last_seen, Duration::minutes(30));
        assert!(parsed.first_seen.is_none());

        sensor.last_updated = "not a timestamp".to_string();
        assert!(SensorRow::from_sensor(&sensor, now).is_none());
    }
}
