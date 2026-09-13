//! Downloading waveform data through the SeismiQ FDSN web service.
//!
//! FDSN `dataselect` is what serves the archive, so it is what a historical
//! download talks to. (SeedLink, the other way in, pushes records as they are
//! recorded; it is the right tool for a live feed, not for fetching a window
//! that has already passed.)
//!
//! The service offers a QuakeSaver-specific `queryauth_jwt_by_id` endpoint that
//! selects streams by sensor UID instead of the usual network/station codes,
//! which is exactly how the rest of this CLI names sensors, and it accepts the
//! access token we already hold.

use chrono::{Local, NaiveDateTime};
use eyre::{bail, eyre, Context, Result};
use futures::StreamExt;
use log::{info, warn};
use std::collections::BTreeMap;

use crate::api::{selected_sensors, SMIQClient, StateConnected};
use crate::cli::{Quality, SensorFilter};
use crate::mseed::{RecordHeader, RecordStream};
use crate::output::{Target, Writer};
use crate::table::print_table;
use crate::timespec::{chunks, fdsn_time, parse_duration, resolve_window};

/// Shared with the live view, which polls the same service.
pub(crate) const FDSN_BASE_URL: &str = "https://fdsnws.network.quakesaver.net";

/// Long downloads are split into requests of at most this much data, cut at UTC
/// midnight so they line up with the day files of an SDS archive.
const DEFAULT_CHUNK: &str = "1d";

/// What the user asked for, straight off the command line.
pub(crate) struct Request<'a> {
    pub(crate) sensors: &'a [String],
    pub(crate) filters: &'a [SensorFilter],
    pub(crate) start: Option<&'a str>,
    pub(crate) end: Option<&'a str>,
    pub(crate) duration: Option<&'a str>,
    pub(crate) output: &'a str,
    pub(crate) quality: Quality,
    pub(crate) minimum_length: f64,
    pub(crate) longest_only: bool,
    pub(crate) chunk: Option<&'a str>,
}

/// What arrived, per stream, for the closing summary.
#[derive(Default)]
struct StreamTally {
    records: usize,
    bytes: usize,
    first: Option<NaiveDateTime>,
    last: Option<NaiveDateTime>,
}

impl StreamTally {
    fn add(&mut self, header: &RecordHeader, bytes: usize) {
        self.records += 1;
        self.bytes += bytes;
        self.first = Some(self.first.map_or(header.start, |f| f.min(header.start)));
        self.last = Some(self.last.map_or(header.start, |l| l.max(header.start)));
    }
}

/// The UID spelling the FDSN endpoint accepts; it joins UIDs with commas, so a
/// UID may only be letters and digits.
fn check_uid(uid: &str) -> Result<()> {
    if uid.is_empty() || !uid.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(eyre!(
            "{:?} is not a sensor UID, expected letters and digits only",
            uid
        ));
    }
    Ok(())
}

/// The UIDs named with `--sensor`, checked over before anything reaches the
/// network.
fn named_sensors(request: &Request<'_>) -> Result<Vec<String>> {
    let mut uids: Vec<String> = Vec::new();
    for uid in request.sensors {
        let uid = uid.trim().to_string();
        check_uid(&uid)?;
        if !uids.contains(&uid) {
            uids.push(uid);
        }
    }
    if uids.is_empty() && request.filters.is_empty() {
        bail!("no sensors selected, name them with --sensor or pick them with --filter");
    }
    Ok(uids)
}

/// Add the sensors `--filter` picks out to the ones already named.
///
/// The account is only queried when a filter asks for it.
async fn resolve_sensors(
    client: &SMIQClient<StateConnected>,
    request: &Request<'_>,
    mut uids: Vec<String>,
) -> Result<Vec<String>> {
    if !request.filters.is_empty() {
        let rows = selected_sensors(client, request.filters)
            .await
            .map_err(|e| eyre!("failed to list sensors: {}", e))?;
        for row in rows {
            if check_uid(&row.uid).is_err() {
                warn!("skipping sensor with an unusable UID {:?}", row.uid);
                continue;
            }
            if !uids.contains(&row.uid) {
                uids.push(row.uid);
            }
        }
    }

    if uids.is_empty() {
        bail!("no sensors matched the selected filters");
    }
    uids.sort();
    Ok(uids)
}

/// Fetch one time window and hand every record it contains to `writer`.
///
/// Returns the number of records written. Records reaching back before
/// `skip_before` were already delivered by the previous window, so they are
/// dropped rather than written twice.
#[allow(clippy::too_many_arguments)]
async fn fetch_window(
    http: &reqwest::Client,
    token: &str,
    uids: &str,
    start: NaiveDateTime,
    end: NaiveDateTime,
    request: &Request<'_>,
    skip_before: Option<NaiveDateTime>,
    writer: &mut Writer,
    tallies: &mut BTreeMap<String, StreamTally>,
) -> Result<usize> {
    let url = format!("{}/fdsnws/dataselect/1/queryauth_jwt_by_id", FDSN_BASE_URL);
    let response = http
        .get(&url)
        .bearer_auth(token)
        .query(&[
            ("starttime", fdsn_time(start)),
            ("endtime", fdsn_time(end)),
            ("sensor_uids", uids.to_string()),
            ("quality", request.quality.to_string()),
            ("minimumlength", request.minimum_length.to_string()),
            ("longestonly", request.longest_only.to_string()),
            ("format", "miniseed".to_string()),
        ])
        .send()
        .await
        .wrap_err("failed to reach the FDSN service")?;

    let status = response.status();
    // 204 is how FDSN says "nothing recorded in that window".
    if status == reqwest::StatusCode::NO_CONTENT {
        info!("{} to {}: no data", fdsn_time(start), fdsn_time(end));
        return Ok(0);
    }
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        bail!(
            "the FDSN service refused the request ({}); check that your account may read these sensors",
            status
        );
    }
    if !status.is_success() {
        let detail = response.text().await.unwrap_or_default();
        bail!(
            "the FDSN service returned {}{}",
            status,
            if detail.is_empty() {
                String::new()
            } else {
                format!(": {}", detail.trim())
            }
        );
    }

    let mut records = RecordStream::new();
    let mut written = 0usize;
    let mut body = response.bytes_stream();
    // The response is streamed and cut into records as it arrives, so a window
    // holding more data than fits in memory still goes through.
    while let Some(chunk) = body.next().await {
        let chunk = chunk.wrap_err("the download was interrupted")?;
        records.push(&chunk);
        while let Some((header, record)) = records.next_record()? {
            if skip_before.is_some_and(|boundary| header.start < boundary) {
                continue;
            }
            tallies
                .entry(header.stream_id())
                .or_default()
                .add(&header, record.len());
            written += 1;
            writer.write_record(&header, record)?;
        }
    }
    records.finish()?;
    Ok(written)
}

pub(crate) async fn download(request: Request<'_>) -> Result<()> {
    let now = Local::now().naive_utc();
    let (start, end) = resolve_window(request.start, request.end, request.duration, now)?;
    let chunk = parse_duration(request.chunk.unwrap_or(DEFAULT_CHUNK))?;

    // Everything that can be settled without the network is settled first, so a
    // bad time range or an unusable UID fails before we ask for a token.
    let named = named_sensors(&request)?;
    let target = Target::from_arg(request.output);

    let client = SMIQClient::new().authenticate().await;
    let uids = resolve_sensors(&client, &request, named).await?;
    let joined = uids.join(",");
    info!(
        "downloading {} to {} for {} sensor(s): {}",
        fdsn_time(start),
        fdsn_time(end),
        uids.len(),
        joined
    );

    let http = reqwest::Client::new();
    let quiet = target.is_stdout();
    let mut writer = Writer::open(target)?;

    let windows = chunks(start, end, chunk);
    let mut tallies: BTreeMap<String, StreamTally> = BTreeMap::new();
    let mut total = 0usize;
    for (index, (window_start, window_end)) in windows.iter().enumerate() {
        // A record straddling a window boundary came with the window before it.
        let skip_before = (index > 0).then_some(*window_start);
        total += fetch_window(
            &http,
            client.get_token(),
            &joined,
            *window_start,
            *window_end,
            &request,
            skip_before,
            &mut writer,
            &mut tallies,
        )
        .await?;
    }

    let files = writer.finish()?;
    report(&tallies, total, files, quiet, request.output);
    Ok(())
}

fn report(
    tallies: &BTreeMap<String, StreamTally>,
    records: usize,
    files: usize,
    quiet: bool,
    output: &str,
) {
    if records == 0 {
        warn!("no data was available for the requested sensors and time range");
        return;
    }
    // Standard output is carrying the miniSEED itself; a table would corrupt it.
    if quiet {
        info!("wrote {} records", records);
        return;
    }

    let rows: Vec<Vec<String>> = tallies
        .iter()
        .map(|(stream, tally)| {
            let time = |t: Option<NaiveDateTime>| {
                t.map(|t| t.format("%Y-%m-%dT%H:%M:%S").to_string())
                    .unwrap_or_default()
            };
            vec![
                stream.clone(),
                tally.records.to_string(),
                format!("{:.1}", tally.bytes as f64 / 1024.0),
                time(tally.first),
                time(tally.last),
            ]
        })
        .collect();
    print_table(&["STREAM", "RECORDS", "KiB", "FIRST", "LAST"], &rows);
    println!(
        "{} records in {} stream(s) written to {} file(s) under {}",
        records,
        tallies.len(),
        files,
        output
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn uids_must_be_alphanumeric() {
        assert!(check_uid("N16EGRY9").is_ok());
        assert!(check_uid("T0IZJPWK").is_ok());
        // Commas separate UIDs in the query, so one inside a UID would smuggle
        // in an extra sensor.
        assert!(check_uid("A,B").is_err());
        assert!(check_uid("../etc").is_err());
        assert!(check_uid("").is_err());
    }

    #[test]
    fn a_tally_tracks_the_span_it_saw() {
        let header = |day: u32| RecordHeader {
            network: "QS".into(),
            station: "BLA".into(),
            location: "".into(),
            channel: "HHZ".into(),
            start: chrono::NaiveDate::from_ymd_opt(2026, 9, day)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            length: 512,
            samples: 100,
            sample_rate: 100.0,
            data_offset: 64,
            encoding: Some(11),
            data_big_endian: true,
        };

        let mut tally = StreamTally::default();
        tally.add(&header(3), 512);
        tally.add(&header(1), 512);
        tally.add(&header(2), 512);

        assert_eq!(tally.records, 3);
        assert_eq!(tally.bytes, 1536);
        assert_eq!(tally.first.unwrap().day(), 1);
        assert_eq!(tally.last.unwrap().day(), 3);
    }
}
