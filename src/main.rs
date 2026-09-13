mod api;
mod cli;
mod fdsn;
mod mseed;
mod output;
mod samples;
mod scan;
mod seedlink;
mod stream;
mod table;
mod timespec;
mod tui;
mod waveforms;
mod websocket;

use crate::scan::scan;
use clap::Parser;

use crate::api::{print_sensors, trigger_action, SMIQClient};
use crate::cli::Commands;
use crate::stream::Transport;
use crate::table::print_table;
use eyre::{bail, eyre, Result};
use std::net::Ipv4Addr;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let args = cli::Cli::parse();
    match args.command {
        Commands::Detect { interface } => {
            let results = scan(interface).await;
            present_results(results);
        }
        Commands::Sensors {
            filter,
            sort,
            reverse,
        } => {
            print_sensors(&filter, sort, reverse).await?;
        }

        Commands::Waveforms {
            sensors,
            filter,
            start,
            end,
            duration,
            output,
            quality,
            minimum_length,
            longest_only,
            chunk,
        } => {
            waveforms::download(waveforms::Request {
                sensors: &sensors,
                filters: &filter,
                start: start.as_deref(),
                end: end.as_deref(),
                duration: duration.as_deref(),
                output: &output,
                quality,
                minimum_length,
                longest_only,
                chunk: chunk.as_deref(),
            })
            .await?;
        }

        Commands::Tui {
            sensor,
            seedlink,
            fdsn,
            window,
            port,
            server,
        } => {
            let chosen = match (seedlink, fdsn) {
                (true, _) => Some(Transport::SeedLink),
                (_, true) => Some(Transport::Fdsn),
                // clap keeps the two flags from being given together.
                _ => None,
            };
            watch(&sensor, chosen, window, port, server).await?;
        }

        Commands::Action { action, sensor_uid } => {
            trigger_action(&action.to_string(), &sensor_uid).await?;
        }
    }
    Ok(())
}

/// How many records may queue up before the reader waits for the view. A
/// sensor sends a handful a second, so this is seconds of slack, not samples.
const UPDATE_QUEUE: usize = 256;

/// Open the live view on one sensor.
async fn watch(
    sensor: &str,
    chosen: Option<Transport>,
    window: f64,
    port: u16,
    server: String,
) -> Result<()> {
    let source = seedlink::Source::from_arg(sensor)?;
    let local = matches!(source, seedlink::Source::Sensor { .. });

    // A sensor named by address is one on this network, and the backend has no
    // way to reach it; its own SeedLink server is the only way in.
    let transport = match chosen {
        Some(transport) => transport,
        None if local => Transport::SeedLink,
        None => Transport::WebSocket,
    };
    if local && transport.needs_uid() {
        bail!(
            "{} is an address on your network, and --{} goes through the backend, \
             which knows sensors by UID. Drop the flag to read the sensor directly, \
             or name it by its UID.",
            sensor,
            transport.name()
        );
    }

    // Everything that needs an account is settled before the screen is taken
    // over, so a missing password prints plainly instead of into a raw
    // terminal. SeedLink asks for no account at all.
    let client = match transport {
        Transport::WebSocket | Transport::Fdsn => {
            api::credentials().map_err(|missing| {
                eyre!(
                    "{} goes through the backend, which needs an account: {}",
                    transport.name(),
                    missing
                )
            })?;
            Some(SMIQClient::new().authenticate().await)
        }
        Transport::SeedLink => None,
    };

    // Logging goes to standard error, which is the same screen the view draws
    // on, so it is silenced for as long as the view is up.
    log::set_max_level(log::LevelFilter::Off);

    let (sender, receiver) = tokio::sync::mpsc::channel(UPDATE_QUEUE);
    let uid = sensor.trim().to_string();
    let reader = match transport {
        Transport::WebSocket => tokio::spawn(websocket::stream(
            client.expect("a websocket needs an account"),
            uid,
            websocket::default_url(),
            sender,
        )),
        Transport::Fdsn => tokio::spawn(fdsn::stream(
            client.expect("the archive needs an account"),
            uid,
            sender,
        )),
        Transport::SeedLink => tokio::spawn(seedlink::stream(source, server, port, sender)),
    };

    let label = format!("{} · {}", sensor.trim(), transport.name());
    let mut terminal = ratatui::init();
    let outcome = tui::run(label, window, receiver, &mut terminal).await;
    ratatui::restore();

    reader.abort();
    log::set_max_level(log::LevelFilter::Info);
    outcome
}

struct Device {
    address: Ipv4Addr,
    uuid: String,
    version: String,
    device_type: String,
}

fn present_results(scan_results: Vec<(Ipv4Addr, String)>) {
    if scan_results.is_empty() {
        println!("no devices detected");
        return;
    }

    let mut devices: Vec<Device> = scan_results
        .iter()
        .map(|(address, body)| parse_device(*address, body))
        .collect();
    devices.sort_by_key(|d| d.address);

    print_device_table(&devices);
}

fn parse_device(address: Ipv4Addr, body: &str) -> Device {
    let json: serde_json::Value = serde_json::from_str(body).unwrap_or(serde_json::Value::Null);
    let field = |key: &str| {
        json.get(key)
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string()
    };
    Device {
        address,
        // The sensor sets its hostname to the device UID (qs-set-hostname).
        uuid: field("hostname"),
        version: field("ringnes"),
        device_type: device_type_from_machine(json.get("machine").and_then(|v| v.as_str())),
    }
}

/// Map the sensor's `machine` string to its product name.
fn device_type_from_machine(machine: Option<&str>) -> String {
    match machine {
        Some(m) if m.starts_with("orange-pi") => "MEMS".to_string(),
        Some(m) if m.starts_with("raspberrypi") => "HiDRA".to_string(),
        Some(m) => m.to_string(),
        None => "unknown".to_string(),
    }
}

fn print_device_table(devices: &[Device]) {
    let rows: Vec<Vec<String>> = devices
        .iter()
        .map(|d| {
            vec![
                d.address.to_string(),
                d.uuid.clone(),
                d.version.clone(),
                d.device_type.clone(),
            ]
        })
        .collect();

    print_table(&["IP ADDRESS", "UUID", "VERSION", "TYPE"], &rows);
}
