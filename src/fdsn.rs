//! The live route through the archive: FDSN dataselect, asked again and again.
//!
//! This is the same service `sqcli waveforms` downloads from, so it needs
//! nothing of the sensor and works wherever the download works. What it cannot
//! do is keep up: data has to be recorded, shipped and archived before the
//! service will hand it back, so the view runs however far behind the archive
//! does. It is the fallback for when the websocket and SeedLink are both out of
//! reach, and useful for watching a sensor that is not streaming at all.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{Local, NaiveDateTime};
use eyre::{bail, Context, Result};
use futures::StreamExt;
use tokio::sync::mpsc::Sender;

use crate::api::{SMIQClient, StateConnected};
use crate::mseed::RecordStream;
use crate::samples::decode;
use crate::stream::{Chunk, Update};
use crate::timespec::fdsn_time;
use crate::waveforms::FDSN_BASE_URL;

/// How often to ask for whatever has landed since the last answer.
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// How much history to ask for on the first request, so the view opens with
/// something on it rather than filling from blank.
const BACKFILL: chrono::Duration = chrono::Duration::minutes(5);

/// The archive is always a little behind, so each request reaches back past
/// where the last one ended rather than trusting the clock.
const OVERLAP: chrono::Duration = chrono::Duration::seconds(30);

/// How long to wait before trying again after a request fails.
const RETRY_DELAY: Duration = Duration::from_secs(5);

/// Fetch one window and forward every record in it that we have not sent
/// already.
///
/// `seen` holds the newest record start per stream, which is what keeps the
/// overlap between requests from being drawn twice.
async fn poll(
    http: &reqwest::Client,
    token: &str,
    uid: &str,
    start: NaiveDateTime,
    end: NaiveDateTime,
    seen: &mut HashMap<String, NaiveDateTime>,
    updates: &Sender<Update>,
) -> Result<usize> {
    let url = format!("{}/fdsnws/dataselect/1/queryauth_jwt_by_id", FDSN_BASE_URL);
    let response = http
        .get(&url)
        .bearer_auth(token)
        .query(&[
            ("starttime", fdsn_time(start)),
            ("endtime", fdsn_time(end)),
            ("sensor_uids", uid.to_string()),
            ("quality", "B".to_string()),
            ("format", "miniseed".to_string()),
        ])
        .send()
        .await
        .wrap_err("failed to reach the FDSN service")?;

    let status = response.status();
    // 204 is how FDSN says "nothing recorded in that window", which for a live
    // view only means the archive has not caught up yet.
    if status == reqwest::StatusCode::NO_CONTENT {
        return Ok(0);
    }
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        bail!(
            "the FDSN service refused the request ({}); check that your account may read {}",
            status,
            uid
        );
    }
    if !status.is_success() {
        bail!("the FDSN service returned {}", status);
    }

    let mut records = RecordStream::new();
    let mut body = response.bytes_stream();
    let mut sent = 0usize;
    while let Some(bytes) = body.next().await {
        let bytes = bytes.wrap_err("the response was interrupted")?;
        records.push(&bytes);
        while let Some((header, record)) = records.next_record()? {
            let stream_id = header.stream_id();
            // Records at or before the newest one we have for this stream came
            // with the overlap and are already on screen.
            if seen.get(&stream_id).is_some_and(|at| header.start <= *at) {
                continue;
            }
            let samples = match decode(&header, record) {
                Ok(samples) => samples,
                Err(e) => {
                    let _ = updates
                        .send(Update::Notice(format!("skipped a record: {}", e)))
                        .await;
                    continue;
                }
            };
            seen.insert(stream_id.clone(), header.start);
            if samples.is_empty() {
                continue;
            }
            sent += 1;
            let chunk = Chunk {
                stream_id,
                channel: header.channel.clone(),
                start: header.start,
                sample_rate: header.sample_rate,
                samples,
            };
            if updates.send(Update::Data(chunk)).await.is_err() {
                return Ok(sent);
            }
        }
    }
    records.finish()?;
    Ok(sent)
}

/// Keep asking the archive for whatever has arrived since last time.
pub(crate) async fn stream(
    client: SMIQClient<StateConnected>,
    uid: String,
    updates: Sender<Update>,
) {
    let http = reqwest::Client::new();
    let mut seen: HashMap<String, NaiveDateTime> = HashMap::new();
    let mut ticks = tokio::time::interval(POLL_INTERVAL);
    let mut from = Local::now().naive_utc() - BACKFILL;
    let mut connected = false;

    loop {
        ticks.tick().await;
        let now = Local::now().naive_utc();
        let outcome = poll(
            &http,
            client.get_token(),
            &uid,
            from,
            now,
            &mut seen,
            &updates,
        )
        .await;

        match outcome {
            Ok(_) => {
                if !connected {
                    connected = true;
                    let sent = updates
                        .send(Update::Connected {
                            server: "FDSN dataselect".to_string(),
                            station: uid.clone(),
                        })
                        .await;
                    if sent.is_err() {
                        return;
                    }
                }
                // The next request reaches back a little, since a record
                // straddling the boundary may not have been archived yet.
                from = now - OVERLAP;
            }
            Err(e) => {
                connected = false;
                if updates
                    .send(Update::Disconnected(format!("{:#}", e)))
                    .await
                    .is_err()
                {
                    return;
                }
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
}
