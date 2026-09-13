//! A SeedLink v3 client, enough of one to follow a single sensor live.
//!
//! SeedLink is how both ends of the fleet hand out data as it is recorded: the
//! network server at `seedlink.network.quakesaver.net` relays every sensor the
//! account may read, and each sensor runs the same protocol itself for whoever
//! can reach it on the LAN. The two speak the same dialect, so one client
//! serves both.
//!
//! The conversation is short. After `HELLO` the client names a station, asks
//! for `DATA`, and says `END`; from then on the server pushes 520 byte packets
//! until the connection drops — an eight byte sequence header followed by one
//! 512 byte miniSEED record.
//!
//! Neither server takes a password. The sensor's own server answers anyone who
//! can reach it, and the network server decides what to hand out from the
//! client's IP address, which an account holder registers under
//! *Waveforms > Network SeedLink Server*.

use std::time::Duration;

use eyre::{bail, eyre, Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

use crate::mseed::parse_header;
use crate::samples::decode;
use crate::stream::{Chunk, Update};

/// The port both the network server and the sensors listen on.
pub(crate) const DEFAULT_PORT: u16 = 18000;

/// SeedLink v3 records are always 512 bytes, behind an eight byte header.
const RECORD_LEN: usize = 512;
const PACKET_HEADER_LEN: usize = 8;
const PACKET_LEN: usize = PACKET_HEADER_LEN + RECORD_LEN;

/// How long to wait before dialling again after the connection drops.
const RECONNECT_DELAY: Duration = Duration::from_secs(3);

/// How long the opening conversation may take before we give up on it. Data
/// arrives on its own schedule afterwards, so only the handshake is timed.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);

/// A station list longer than this is not one we can use, and reading on
/// forever is worse than reporting what we have.
const MAX_INFO_PACKETS: usize = 512;

/// Where to get the live data, and how to ask for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Source {
    /// The network server, which knows every sensor by its UID and hands out
    /// the ones the client's IP address is registered for.
    Network { uid: String },
    /// A sensor's own server, which serves only itself and so needs no name.
    Sensor { host: String },
}

impl Source {
    /// Read the target off the command line.
    ///
    /// Sensor UIDs are letters and digits only, so anything carrying a dot or a
    /// colon is an address rather than a UID.
    pub(crate) fn from_arg(arg: &str) -> Result<Self> {
        let arg = arg.trim();
        if arg.is_empty() {
            bail!("no sensor given");
        }
        if arg.contains('.') || arg.contains(':') {
            return Ok(Source::Sensor {
                host: arg.to_string(),
            });
        }
        if !arg.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(eyre!(
                "{:?} is neither a sensor UID nor an address, expected letters and digits or a host like 192.168.1.20",
                arg
            ));
        }
        Ok(Source::Network {
            uid: arg.to_string(),
        })
    }

    /// The host and port to dial.
    fn address(&self, network_host: &str, port: u16) -> (String, u16) {
        match self {
            Source::Network { .. } => (network_host.to_string(), port),
            Source::Sensor { host } => match host.rsplit_once(':') {
                // An explicit port in the address wins over the default.
                Some((host, given)) if given.parse::<u16>().is_ok() => {
                    (host.to_string(), given.parse().expect("checked just above"))
                }
                _ => (host.clone(), port),
            },
        }
    }

    /// The station to ask for. A sensor answers `*` with its own station code,
    /// which saves us having to know how it was configured; the network server
    /// serves many, so there it has to be named.
    fn station(&self) -> &str {
        match self {
            Source::Network { uid } => uid,
            Source::Sensor { .. } => "*",
        }
    }
}

/// Send one command and read the `OK` the server answers with.
async fn command(stream: &mut BufReader<TcpStream>, command: &str) -> Result<()> {
    stream
        .write_all(format!("{}\r\n", command).as_bytes())
        .await
        .wrap_err_with(|| format!("failed to send {:?}", command))?;
    stream.flush().await?;

    let reply = read_line(stream).await?;
    match reply.as_str() {
        "OK" => Ok(()),
        "ERROR" => Err(eyre!("the server refused {:?}", command)),
        other => Err(eyre!(
            "the server answered {:?} with {:?}, which is not SeedLink",
            command,
            other
        )),
    }
}

/// Read one `\r\n` terminated line of the handshake.
async fn read_line(stream: &mut BufReader<TcpStream>) -> Result<String> {
    let mut line = Vec::new();
    let read = stream
        .read_until(b'\n', &mut line)
        .await
        .wrap_err("the connection dropped mid-handshake")?;
    if read == 0 {
        bail!("the server closed the connection during the handshake");
    }
    Ok(String::from_utf8_lossy(&line)
        .trim_matches(|c| c == '\r' || c == '\n')
        .to_string())
}

/// Greet the server and read the two lines it names itself with.
async fn hello(stream: &mut BufReader<TcpStream>) -> Result<String> {
    stream.write_all(b"HELLO\r\n").await?;
    stream.flush().await?;
    let software = read_line(stream).await?;
    // The second line is the organisation, which we only read to stay in step.
    let _organisation = read_line(stream).await?;
    if !software.starts_with("SeedLink") {
        bail!("{:?} does not look like a SeedLink server", software);
    }
    Ok(software)
}

/// Pull the station names out of an `INFO STATIONS` answer.
///
/// The reply is XML wrapped in miniSEED records, and all we want from it is the
/// list of names, so it is scanned rather than parsed.
fn station_names(xml: &str) -> Vec<String> {
    xml.split("<station")
        .skip(1)
        .filter_map(|station| {
            let rest = station.split_once("name=\"")?.1;
            let (name, _) = rest.split_once('"')?;
            Some(name.to_string())
        })
        .collect()
}

/// Ask which stations the server will hand out on this connection.
///
/// The network server answers with the sensors the client's IP address is
/// registered for, which is the one thing worth knowing before waiting on a
/// stream that may never start.
async fn stations(stream: &mut BufReader<TcpStream>) -> Result<Vec<String>> {
    stream.write_all(b"INFO STATIONS\r\n").await?;
    stream.flush().await?;

    let mut xml = String::new();
    // The answer arrives as ordinary packets carrying XML rather than samples.
    // Only the station elements are wanted, so the records are scanned whole
    // instead of being parsed as miniSEED.
    for _ in 0..MAX_INFO_PACKETS {
        let mut packet = [0u8; PACKET_LEN];
        stream
            .read_exact(&mut packet)
            .await
            .wrap_err("the connection dropped while listing stations")?;
        if !packet.starts_with(b"SLINFO") {
            bail!("the server sent data instead of a station list");
        }
        xml.push_str(&String::from_utf8_lossy(&packet[PACKET_HEADER_LEN..]).replace('\0', ""));
        if xml.contains("</seedlink>") {
            break;
        }
    }
    Ok(station_names(&xml))
}

/// Turn one record into the samples it carries.
fn chunk_from_record(record: &[u8]) -> Result<Option<Chunk>> {
    let Some(header) = parse_header(record)? else {
        bail!("a packet held less than a whole record");
    };
    let samples = decode(&header, record)?;
    // Log and status records ride the same streams as waveforms.
    if samples.is_empty() {
        return Ok(None);
    }
    Ok(Some(Chunk {
        stream_id: header.stream_id(),
        channel: header.channel.clone(),
        start: header.start,
        sample_rate: header.sample_rate,
        samples,
    }))
}

/// Run one connection: handshake, then packets until it drops.
async fn session(source: &Source, host: &str, port: u16, updates: &Sender<Update>) -> Result<()> {
    let stream = TcpStream::connect((host, port))
        .await
        .map_err(|e| match source {
            // A sensor ships with its server switched off, which is by far the
            // likeliest reason it is not answering.
            Source::Sensor { .. } => eyre!(
                "could not reach {}:{} ({}). A sensor only serves SeedLink once it is \
                 started under Waveform access > SeedLink Server in its web interface.",
                host,
                port,
                e
            ),
            Source::Network { .. } => eyre!("could not reach {}:{}: {}", host, port, e),
        })?;
    // Records arrive in 520 byte packets, so there is no point buffering less.
    let mut stream = BufReader::with_capacity(PACKET_LEN * 8, stream);

    // The opening conversation is the part that can hang on a server that
    // accepts the connection and then says nothing; the data that follows
    // arrives on its own schedule and is left untimed.
    let opening = async {
        let software = hello(&mut stream).await?;
        let station = source.station();

        // A list we could not read says nothing either way, so the check is only
        // made when the server actually answered.
        let available = stations(&mut stream).await.ok();
        if let (Source::Network { uid }, Some(available)) = (source, &available) {
            if !available.iter().any(|name| name == uid) {
                if available.is_empty() {
                    bail!(
                        "{} serves no sensors to this machine. The network server \
                         recognises an account by the address it connects from, so add \
                         this machine's public IP under Waveforms > Network SeedLink \
                         Server, or point sqcli at a sensor on your own network instead.",
                        host
                    );
                }
                bail!(
                    "{} does not serve sensor {}; it offers {}",
                    host,
                    uid,
                    available.join(", ")
                );
            }
        }

        command(&mut stream, &format!("STATION {}", station)).await?;
        // Without a SELECT the server sends every channel the station has, which is
        // what the stacked view wants.
        command(&mut stream, "DATA").await?;
        stream.write_all(b"END\r\n").await?;
        stream.flush().await?;

        let named = match source {
            Source::Network { uid } => uid.clone(),
            Source::Sensor { .. } => available
                .and_then(|stations| stations.first().cloned())
                .unwrap_or_else(|| "*".into()),
        };
        Ok::<(String, String), eyre::Report>((software, named))
    };
    let (software, named) = tokio::time::timeout(HANDSHAKE_TIMEOUT, opening)
        .await
        .map_err(|_| {
            eyre!(
                "{} accepted the connection but did not finish the SeedLink handshake within {}s",
                host,
                HANDSHAKE_TIMEOUT.as_secs()
            )
        })??;

    let _ = updates
        .send(Update::Connected {
            server: software,
            station: named,
        })
        .await;

    loop {
        let mut packet = [0u8; PACKET_LEN];
        match stream.read_exact(&mut packet).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                bail!("the server closed the stream");
            }
            Err(e) => return Err(e).wrap_err("the stream dropped"),
        }

        if packet.starts_with(b"SLINFO") {
            // An info answer we did not ask for; nothing to draw.
            continue;
        }
        if !packet.starts_with(b"SL") {
            bail!("the server sent something that is not a SeedLink packet");
        }

        match chunk_from_record(&packet[PACKET_HEADER_LEN..]) {
            // A single unreadable record should not take the view down with it.
            Err(e) => {
                let _ = updates
                    .send(Update::Notice(format!("skipped a record: {}", e)))
                    .await;
            }
            Ok(None) => {}
            Ok(Some(chunk)) => {
                if updates.send(Update::Data(chunk)).await.is_err() {
                    // The interface is gone, so there is nobody left to feed.
                    return Ok(());
                }
            }
        }
    }
}

/// Keep a live connection going, dialling again whenever it drops.
///
/// Runs until the interface hangs up its end of `updates`.
pub(crate) async fn stream(
    source: Source,
    network_host: String,
    port: u16,
    updates: Sender<Update>,
) {
    let (host, port) = source.address(&network_host, port);
    loop {
        let outcome = session(&source, &host, port, &updates).await;
        let reason = match outcome {
            // A clean return means the interface closed; there is nothing to
            // reconnect for.
            Ok(()) => return,
            Err(e) => format!("{:#}", e),
        };
        if updates.send(Update::Disconnected(reason)).await.is_err() {
            return;
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_uid_goes_to_the_network_and_an_address_to_the_sensor() {
        // Sensor UIDs are letters and digits, so a dot or a colon is the thing
        // that tells an address apart from one.
        assert_eq!(
            Source::from_arg("A3B7K9Q2").unwrap(),
            Source::Network {
                uid: "A3B7K9Q2".into()
            }
        );
        assert_eq!(
            Source::from_arg("192.168.178.55").unwrap(),
            Source::Sensor {
                host: "192.168.178.55".into()
            }
        );
        assert_eq!(
            Source::from_arg("qssensor.local").unwrap(),
            Source::Sensor {
                host: "qssensor.local".into()
            }
        );
        assert!(Source::from_arg("").is_err());
        assert!(Source::from_arg("not a sensor").is_err());
    }

    #[test]
    fn the_network_server_is_only_dialled_for_a_uid() {
        let network = Source::from_arg("A3B7K9Q2").unwrap();
        assert_eq!(
            network.address("seedlink.example.net", 18000),
            ("seedlink.example.net".to_string(), 18000)
        );

        let sensor = Source::from_arg("192.168.178.55").unwrap();
        assert_eq!(
            sensor.address("seedlink.example.net", 18000),
            ("192.168.178.55".to_string(), 18000)
        );
    }

    #[test]
    fn a_port_in_the_address_wins_over_the_default() {
        let sensor = Source::from_arg("192.168.178.55:18010").unwrap();
        assert_eq!(
            sensor.address("ignored", 18000),
            ("192.168.178.55".to_string(), 18010)
        );
    }

    #[test]
    fn a_sensor_is_asked_for_its_own_station() {
        // A sensor serves only itself, so `*` saves us knowing how its station
        // code was configured. The network server serves many and needs naming.
        assert_eq!(Source::from_arg("10.0.0.5").unwrap().station(), "*");
        assert_eq!(Source::from_arg("A3B7K9Q2").unwrap().station(), "A3B7K9Q2");
    }

    #[test]
    fn station_names_are_picked_out_of_the_info_answer() {
        let xml = r#"<?xml version="1.0"?><seedlink software="SeedLink v3.0" organization="QuakeSaver">
            <station name="A3B7K9Q2" network="QS" description="one" begin_seq="0" end_seq="1" />
            <station name="XYZ12345" network="QS" description="two" begin_seq="0" end_seq="1" />
            </seedlink>"#;

        assert_eq!(station_names(xml), ["A3B7K9Q2", "XYZ12345"]);
    }

    #[test]
    fn an_empty_station_list_is_not_mistaken_for_a_station() {
        let xml = r#"<?xml version="1.0"?><seedlink software="SeedLink v3.0"></seedlink>"#;
        assert!(station_names(xml).is_empty());
    }
}
