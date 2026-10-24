use cidr::Ipv4Cidr;
use futures::future::join_all;
use local_ip_address::{list_afinet_netifas, local_ip};
use log::{debug, info};
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;
use tokio::time::timeout;

pub async fn scan(interface: Option<String>) -> Vec<(Ipv4Addr, String)> {
    let local_ip = get_local_ip(interface);

    info!("local IP address: {:?}", local_ip);
    let ip_address = to_ipv4_address(local_ip);
    let ipv4 = to_base_address(ip_address);

    let ip_range = Ipv4Cidr::new(ipv4, 24).unwrap();
    let results = probe_ip_range(ip_range).await;
    debug!("results: {:?}", results);
    results
}

/// Probe a single host. Returns `Some((address, body))` only when the host
/// actually answered on `port`; `None` means no device there.
async fn probe_host(
    client: reqwest::Client,
    address: Ipv4Addr,
    port: u16,
) -> Option<(Ipv4Addr, String)> {
    match client
        .get(format!("http://{}:{}", address, port))
        .send()
        .await
    {
        Ok(response) => response.text().await.ok().map(|body| (address, body)),
        Err(e) => {
            debug!("error: {}", e);
            None
        }
    }
}

async fn probe_ip_range(ip_range: Ipv4Cidr) -> Vec<(Ipv4Addr, String)> {
    // One client for all hosts. `reqwest::get` builds a client per call, which
    // loads the TLS roots each time, and `join_all` builds them one after the
    // other while every timeout is already running: hosts past the first few
    // dozen timed out before their request was even sent.
    let client = reqwest::Client::new();
    let mut futures = Vec::new();
    for ip in ip_range {
        debug!("probing {:?}", ip);
        futures.push(timeout(
            Duration::from_millis(2000),
            probe_host(client.clone(), ip.address(), 27740),
        ));
    }
    let results = join_all(futures).await;
    results
        .into_iter()
        // Drop both timeouts (`Err`) and non-responders (`Ok(None)`).
        .filter_map(|result| result.ok().flatten())
        .collect()
}

fn to_base_address(ip_address: Ipv4Addr) -> Ipv4Addr {
    let mut octets = ip_address.octets();
    octets[3] = 0;
    Ipv4Addr::from(octets)
}

fn to_ipv4_address(local_ip: IpAddr) -> Ipv4Addr {
    match local_ip {
        IpAddr::V4(ip4) => ip4,
        IpAddr::V6(_) => panic!("IPv6 addresses are not supported"),
    }
}

fn get_local_ip(interface: Option<String>) -> IpAddr {
    let local_ip = match interface {
        None => local_ip().unwrap(),
        Some(interface) => {
            let network_interfaces = list_afinet_netifas().unwrap();
            // An interface is listed once per address (e.g. an IPv6 link-local
            // next to its IPv4 one); the scan needs the IPv4 address.
            let (_, ip) = network_interfaces
                .iter()
                .find(|(name, ip)| *name == interface && ip.is_ipv4())
                .unwrap_or_else(|| panic!("no IPv4 address found on interface {}", interface));
            *ip
        }
    };
    local_ip
}
