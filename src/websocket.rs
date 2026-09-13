//! The live route the web frontend uses: a websocket through the backend.
//!
//! The backend relays what the sensors publish, so this reaches any sensor the
//! account may read without a direct line to it and without the IP
//! registration SeedLink's relay asks for. It is what `sqcli tui` uses unless
//! another transport is named.
//!
//! Two things have to happen for samples to flow. The client subscribes to the
//! sensor's `WaveformData` data product over the websocket, and the sensor is
//! told to start streaming with a `StreamWaveformsTrigger` action. A sensor
//! stops streaming a minute after it was last asked, so the trigger is repeated
//! for as long as the view is open.
//!
//! Samples arrive per channel as base64, gzipped unless the frame says
//! otherwise, holding little-endian 32 bit integers for raw counts and floats
//! for anything already converted to ground motion.

use std::time::Duration;

use base64::prelude::{Engine, BASE64_STANDARD};
use chrono::{DateTime, NaiveDateTime};
use eyre::{bail, eyre, Context, Result};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::tungstenite::Message;

use crate::api::{SMIQClient, StateConnected};
use crate::stream::{Chunk, Update};

/// The websocket the frontend talks to, and the data product carrying samples.
const WS_PATH: &str = "wss://api.network.quakesaver.net/api/v1/ws";
const WAVEFORM_PRODUCT: &str = "WaveformData";

/// The action that makes a sensor stream, and how often to repeat it. A sensor
/// gives up a minute after the last trigger, so this leaves room to spare.
const STREAM_ACTION: &str = "StreamWaveformsTrigger";
const RETRIGGER_INTERVAL: Duration = Duration::from_secs(45);

/// How long to wait before trying again after the connection drops.
const RECONNECT_DELAY: Duration = Duration::from_secs(3);

/// A message the backend sends down the socket.
///
/// Only the data frames matter here; the rest are read far enough to report a
/// refusal and otherwise ignored.
#[derive(Debug, Deserialize)]
struct ServerMessage {
    #[serde(default)]
    r#type: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    sensor_uid: Option<String>,
    #[serde(default)]
    data: Option<DataProduct>,
}

#[derive(Debug, Deserialize)]
struct DataProduct {
    #[serde(default)]
    name: String,
    #[serde(default)]
    traces: Option<Traces>,
}

/// A frame of waveform, one entry per channel.
#[derive(Debug, Deserialize)]
struct Traces {
    /// Base64 per channel, gzipped when `compressed`.
    data: std::collections::BTreeMap<String, String>,
    /// Seconds between samples.
    delta_t: f64,
    /// When the *last* sample of the frame was recorded; the start is worked
    /// back from the sample count.
    endtime: String,
    #[serde(default)]
    data_unit: Option<String>,
    #[serde(default)]
    compressed: bool,
}

/// Read the frame's end time, which arrives with or without a zone.
fn parse_endtime(text: &str) -> Result<NaiveDateTime> {
    if let Ok(fixed) = DateTime::parse_from_rfc3339(text) {
        return Ok(fixed.naive_utc());
    }
    NaiveDateTime::parse_from_str(text.trim_end_matches(['Z', 'z']), "%Y-%m-%dT%H:%M:%S%.f")
        .wrap_err_with(|| format!("cannot read the frame's end time {:?}", text))
}

/// Turn one channel's payload into samples.
///
/// The unit decides the width: raw counts are 32 bit integers, anything already
/// converted to ground motion is 32 bit floats. Both are little-endian, the
/// order the sensors write them in.
fn decode_channel(encoded: &str, compressed: bool, unit: &str) -> Result<Vec<f64>> {
    let raw = BASE64_STANDARD
        .decode(encoded)
        .wrap_err("a channel's samples were not valid base64")?;
    let bytes = if compressed {
        use std::io::Read;
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(raw.as_slice())
            .read_to_end(&mut out)
            .wrap_err("a channel's samples would not decompress")?;
        out
    } else {
        raw
    };

    if bytes.len() % 4 != 0 {
        bail!(
            "a channel carried {} bytes, not a whole number of samples",
            bytes.len()
        );
    }
    let counts = unit == "counts";
    Ok(bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|raw| {
            let word = u32::from_le_bytes(*raw);
            if counts {
                word as i32 as f64
            } else {
                f32::from_bits(word) as f64
            }
        })
        .collect())
}

/// Split a waveform frame into one chunk per channel.
fn chunks_from_traces(uid: &str, traces: &Traces) -> Result<Vec<Chunk>> {
    if traces.delta_t <= 0.0 {
        bail!(
            "frame states an impossible sample spacing of {}",
            traces.delta_t
        );
    }
    let endtime = parse_endtime(&traces.endtime)?;
    let unit = traces.data_unit.as_deref().unwrap_or("counts");
    let rate = 1.0 / traces.delta_t;

    let mut chunks = Vec::new();
    for (channel, encoded) in &traces.data {
        let samples = decode_channel(encoded, traces.compressed, unit)?;
        if samples.is_empty() {
            continue;
        }
        // The frame is stamped at its last sample, so the first one sits a
        // frame-length earlier.
        let span = (samples.len() - 1) as f64 * traces.delta_t;
        let start = endtime - chrono::Duration::nanoseconds((span * 1e9) as i64);
        chunks.push(Chunk {
            // The backend names the sensor, not a full SEED stream, so the
            // panels are keyed by sensor and channel.
            stream_id: format!("{}.{}", uid, channel),
            channel: channel.clone(),
            start,
            sample_rate: rate,
            samples,
        });
    }
    Ok(chunks)
}

/// Ask the sensor to keep streaming, and say so if it will not.
async fn trigger(client: &SMIQClient<StateConnected>, uid: &str) -> Result<()> {
    sensor_api::apis::sensors_api::trigger_sensor_action(
        &client.api_configuration(),
        uid,
        STREAM_ACTION,
        json!({}),
    )
    .await
    .map_err(|e| eyre!("could not ask {} to start streaming: {}", uid, e))?;
    Ok(())
}

/// Run one connection: subscribe, keep the sensor streaming, and forward frames.
async fn session(
    client: &SMIQClient<StateConnected>,
    uid: &str,
    url: &str,
    updates: &Sender<Update>,
) -> Result<()> {
    let target = format!("{}?token={}", url, client.get_token());
    let (mut socket, _) = tokio_tungstenite::connect_async(&target)
        .await
        .map_err(|e| {
            // The backend turns any rejected token into a plain 403, so that is
            // the one failure worth explaining rather than repeating.
            if e.to_string().contains("403") {
                eyre!(
                    "the backend refused the websocket (403); the access token was not \
                     accepted, so check SEISMIQ_USERNAME and SEISMIQ_PASSWORD"
                )
            } else {
                eyre!("could not open the websocket to the backend: {}", e)
            }
        })?;

    let subscribe = json!({
        "type": "request",
        "topic": "data_product",
        "method": "subscribe",
        "sensor_uids": [uid],
        "data_product_name": WAVEFORM_PRODUCT,
    });
    socket
        .send(Message::Text(subscribe.to_string().into()))
        .await
        .wrap_err("could not subscribe to the sensor's waveforms")?;

    // A sensor only streams while it is being asked to, so the first ask goes
    // out now and the rest on a timer.
    trigger(client, uid).await?;
    let mut retrigger = tokio::time::interval(RETRIGGER_INTERVAL);
    retrigger.tick().await;

    let _ = updates
        .send(Update::Connected {
            server: "backend websocket".to_string(),
            station: uid.to_string(),
        })
        .await;

    loop {
        tokio::select! {
            _ = retrigger.tick() => {
                if let Err(e) = trigger(client, uid).await {
                    let _ = updates.send(Update::Notice(format!("{:#}", e))).await;
                }
            }
            frame = socket.next() => {
                let Some(frame) = frame else {
                    bail!("the backend closed the websocket");
                };
                let frame = frame.wrap_err("the websocket dropped")?;
                let text = match frame {
                    Message::Text(text) => text.to_string(),
                    Message::Binary(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                    Message::Close(_) => bail!("the backend closed the websocket"),
                    // Ping and pong are answered by the library.
                    _ => continue,
                };

                let message: ServerMessage = match serde_json::from_str(&text) {
                    Ok(message) => message,
                    Err(e) => {
                        let _ = updates
                            .send(Update::Notice(format!("unreadable frame: {}", e)))
                            .await;
                        continue;
                    }
                };

                if message.r#type == "status" {
                    if message.status.as_deref() == Some("ERROR") {
                        bail!(
                            "the backend refused the subscription: {}",
                            message.message.unwrap_or_else(|| "no reason given".into())
                        );
                    }
                    continue;
                }
                let Some(product) = message.data else { continue };
                if product.name != WAVEFORM_PRODUCT {
                    continue;
                }
                let Some(traces) = product.traces else { continue };
                let named = message.sensor_uid.unwrap_or_else(|| uid.to_string());

                match chunks_from_traces(&named, &traces) {
                    // One bad frame should not take the view down.
                    Err(e) => {
                        let _ = updates
                            .send(Update::Notice(format!("skipped a frame: {:#}", e)))
                            .await;
                    }
                    Ok(chunks) => {
                        for chunk in chunks {
                            if updates.send(Update::Data(chunk)).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Keep the websocket going, opening it again whenever it drops.
pub(crate) async fn stream(
    client: SMIQClient<StateConnected>,
    uid: String,
    url: String,
    updates: Sender<Update>,
) {
    loop {
        let reason = match session(&client, &uid, &url, &updates).await {
            // A clean return means the view closed.
            Ok(()) => return,
            Err(e) => format!("{:#}", e),
        };
        if updates.send(Update::Disconnected(reason)).await.is_err() {
            return;
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

/// The websocket the backend serves, for the command line default.
pub(crate) fn default_url() -> String {
    WS_PATH.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::io::Write;

    /// Samples the way a sensor sends them: little-endian, base64, gzipped.
    fn encode(samples: &[i32], compress: bool) -> String {
        let raw: Vec<u8> = samples.iter().flat_map(|v| v.to_le_bytes()).collect();
        let bytes = if compress {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(&raw).expect("in-memory write");
            encoder.finish().expect("in-memory finish")
        } else {
            raw
        };
        BASE64_STANDARD.encode(bytes)
    }

    fn traces(data: BTreeMap<String, String>, compressed: bool, unit: &str) -> Traces {
        Traces {
            data,
            delta_t: 0.01,
            endtime: "2026-09-13T12:00:01".to_string(),
            data_unit: Some(unit.to_string()),
            compressed,
        }
    }

    #[test]
    fn counts_are_read_as_little_endian_integers() {
        let encoded = encode(&[1, -2, 300_000], false);
        assert_eq!(
            decode_channel(&encoded, false, "counts").unwrap(),
            vec![1.0, -2.0, 300_000.0]
        );
    }

    #[test]
    fn gzipped_samples_are_unpacked_first() {
        let encoded = encode(&[7, 8, 9], true);
        assert_eq!(
            decode_channel(&encoded, true, "counts").unwrap(),
            vec![7.0, 8.0, 9.0]
        );
    }

    #[test]
    fn ground_motion_arrives_as_floats_rather_than_counts() {
        // The unit decides how the same four bytes are read, so a frame in m/s
        // read as counts would come out as nonsense rather than as an error.
        let raw: Vec<u8> = [1.5f32, -0.25]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let encoded = BASE64_STANDARD.encode(raw);

        assert_eq!(
            decode_channel(&encoded, false, "m/s").unwrap(),
            vec![1.5, -0.25]
        );
        assert_ne!(
            decode_channel(&encoded, false, "counts").unwrap(),
            vec![1.5, -0.25]
        );
    }

    #[test]
    fn a_payload_that_is_not_whole_samples_is_rejected() {
        let encoded = BASE64_STANDARD.encode([1u8, 2, 3]);
        assert!(decode_channel(&encoded, false, "counts").is_err());
        assert!(decode_channel("not base64!", false, "counts").is_err());
    }

    #[test]
    fn the_frame_is_stamped_at_its_last_sample() {
        let mut data = BTreeMap::new();
        // Ten samples at 100 Hz span 0.09 s from first to last.
        data.insert("EHZ".to_string(), encode(&[0; 10], true));
        let chunks = chunks_from_traces("A3B7K9Q2", &traces(data, true, "counts")).unwrap();

        assert_eq!(chunks.len(), 1);
        let chunk = &chunks[0];
        assert_eq!(chunk.sample_rate, 100.0);
        assert_eq!(chunk.samples.len(), 10);
        assert_eq!(
            chunk.start,
            NaiveDateTime::parse_from_str("2026-09-13T12:00:00.91", "%Y-%m-%dT%H:%M:%S%.f")
                .unwrap()
        );
    }

    #[test]
    fn every_channel_of_a_frame_becomes_its_own_panel() {
        let mut data = BTreeMap::new();
        for channel in ["EHE", "EHN", "EHZ"] {
            data.insert(channel.to_string(), encode(&[1, 2, 3], false));
        }
        let chunks = chunks_from_traces("A3B7K9Q2", &traces(data, false, "counts")).unwrap();

        let ids: Vec<&str> = chunks.iter().map(|c| c.stream_id.as_str()).collect();
        assert_eq!(ids, ["A3B7K9Q2.EHE", "A3B7K9Q2.EHN", "A3B7K9Q2.EHZ"]);
        assert!(chunks.iter().all(|c| c.samples == vec![1.0, 2.0, 3.0]));
    }

    #[test]
    fn an_end_time_is_read_with_or_without_a_zone() {
        let bare = parse_endtime("2026-09-13T12:00:01").unwrap();
        assert_eq!(parse_endtime("2026-09-13T12:00:01Z").unwrap(), bare);
        // An offset is carried back to UTC rather than dropped.
        assert_eq!(parse_endtime("2026-09-13T14:00:01+02:00").unwrap(), bare);
        assert!(parse_endtime("some other day").is_err());
    }

    #[test]
    fn a_frame_without_a_sample_spacing_is_rejected() {
        let mut data = BTreeMap::new();
        data.insert("EHZ".to_string(), encode(&[1, 2], false));
        let mut frame = traces(data, false, "counts");
        frame.delta_t = 0.0;

        assert!(chunks_from_traces("A3B7K9Q2", &frame).is_err());
    }

    #[test]
    fn a_data_frame_is_recognised_by_its_shape() {
        // The exact wire format the backend sends, so a rename upstream shows
        // up here rather than as an empty screen.
        let text = r#"{"type":"data","topic":"data_product","sensor_uid":"A3B7K9Q2",
            "data":{"name":"WaveformData","traces":{"data":{"EHZ":"AAAAAA=="},
            "delta_t":0.01,"endtime":"2026-09-13T12:00:01","data_unit":"counts",
            "compressed":false}}}"#;
        let message: ServerMessage = serde_json::from_str(text).expect("parses");

        assert_eq!(message.r#type, "data");
        assert_eq!(message.sensor_uid.as_deref(), Some("A3B7K9Q2"));
        let product = message.data.expect("a data product");
        assert_eq!(product.name, WAVEFORM_PRODUCT);
        assert!(product.traces.is_some());
    }

    #[test]
    fn a_refusal_from_the_backend_is_readable() {
        let text = r#"{"type":"status","status":"ERROR","message":"no permission"}"#;
        let message: ServerMessage = serde_json::from_str(text).expect("parses");

        assert_eq!(message.r#type, "status");
        assert_eq!(message.status.as_deref(), Some("ERROR"));
        assert_eq!(message.message.as_deref(), Some("no permission"));
    }
}
