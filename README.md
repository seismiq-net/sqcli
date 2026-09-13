# Seismiq Command Line Interface

# Installation

Download the artifact for your operating system from the
[latest release](https://github.com/seismiq-net/sqcli/releases), then follow
the steps below. Replace `<version>` (e.g. `1.1.0`) with the release you
downloaded.

## Debian / Ubuntu

Install the `.deb` package. It places `sqcli` on your `PATH` automatically:

```shell
sudo apt install ./sqcli-<version>.deb
# or: sudo dpkg -i sqcli-<version>.deb
```

## Linux (other distributions, x86_64)

Download the bare `sqcli` binary, make it executable, and move it onto your
`PATH`:

```shell
chmod +x sqcli
sudo mv sqcli /usr/local/bin/sqcli
```

## macOS (Apple Silicon)

Download `sqcli-osx`, make it executable, and move it onto your `PATH`. macOS
quarantines files downloaded from the browser, so clear that flag as well:

```shell
chmod +x sqcli-osx
xattr -d com.apple.quarantine sqcli-osx   # only needed for browser downloads
sudo mv sqcli-osx /usr/local/bin/sqcli
```

## Windows

Download `sqcli.exe` and place it in a folder that is on your `PATH` (or add its
folder to `PATH`). You can then run `sqcli` from PowerShell or Command Prompt.

# Usage

The `sensors`, `waveforms`, `tui` and `action` commands talk to the SeismiQ
cloud API and need credentials. Set them via two environment variables (a local `.env`
file is also picked up):

```shell
export SEISMIQ_USERNAME="your username" 
export SEISMIQ_PASSWORD="your password"
```

The `detect` command talks directly to devices on your LAN and needs no
authentication. Neither does `tui` when it reads a sensor on your own network,
or over SeedLink (see [`tui`](#tui-sensor)).

Get a list of available commands:

```shell
sqcli -h
```

## Commands

### `detect [interface]`

Discover SeismiQ sensors on your **local network**. It probes every host in
your machine's `/24` subnet on port `27740` and prints the ones that answer.
By default it uses your primary network interface; pass an interface name
(e.g. `en0`, `eth0`) to base the scan on a different one.

```shell
sqcli detect
```

```text
IP ADDRESS      UUID     VERSION  TYPE
192.168.178.55  A3B7K9Q2  dev      MEMS
192.168.178.89  unknown   dev      HiDRA
```

### `sensors [-f <filter>...] [-s <column>] [-r]`

List the sensors registered to your SeismiQ account, fetched from the cloud
API. Requires authentication. Every sensor is listed unless you filter the
list. Each row shows an icon for the hardware revision, the sensor UID, its
hardware family, whether it is online, its software version, how long ago it
was last seen, and its number of warnings.

```shell
sqcli sensors
```

```text
    UID         TYPE     STATUS   VERSION     LAST SEEN  WARN
▣   A3B7K9Q2    MEMS     online   1.0.0       5m 0s
🌀  XYZ12345    HiDRA    offline  1.12.0-rc1  3d 1h
🌋  LONGERUID1  MEMS     offline  1.0.0       1h 15m     3
```

#### Filtering

Pass `--filter`/`-f` to narrow the list. Repeat the flag or comma-separate the
values:

- `online` — seen within the last hour.
- `offline` — not seen within the last hour.
- `mems` — MEMS sensors (ADXL or BMA accelerometer).
- `hidra` — HiDRA sensors.
- `unknown` — sensors with a hardware revision `sqcli` does not recognise.
- `warnings` — sensors carrying at least one warning.

Filters of the same kind widen the selection, filters of different kinds narrow
it. So `-f mems -f hidra` lists both hardware families, while
`-f mems,hidra -f online` lists only the online ones among them:

```shell
sqcli sensors -f online              # what the bare command used to show
sqcli sensors -f offline -f hidra    # HiDRA units that dropped off
sqcli sensors -f warnings            # anything reporting a problem
```

#### Sorting

Pass `--sort`/`-s` to order the list by `uid` (the default), `last-seen`,
`first-seen`, `version`, `type`, or `warnings`. Add `--reverse`/`-r` to flip
the order. Sensors sharing a value are ordered by UID.

```shell
sqcli sensors -s last-seen           # freshest first
sqcli sensors -s last-seen -r        # longest silent first
sqcli sensors -f offline -s version  # stale units, grouped by firmware
```

### `waveforms -o <path> [-u <uid>...] [-f <filter>...] [-d <duration>]`

Download seismic waveform data as miniSEED. Requires authentication. The data
comes from the SeismiQ FDSN web service (`fdsnws.network.quakesaver.net`), which
serves the recorded archive, so historic windows work as well as recent ones.

```shell
sqcli waveforms -u A3B7K9Q2 -d 10m -o last10min.mseed
```

#### Selecting sensors

Name sensors with `--sensor`/`-u`, or let the same filters as `sqcli sensors`
pick them. Both may be combined, in which case everything they select is
downloaded:

```shell
sqcli waveforms -u A3B7K9Q2,XYZ12345 -d 1h -o out.mseed   # two named sensors
sqcli waveforms -f hidra,mems -f online -d 1h -o out.mseed # whatever is online
```

At least one of the two is required, so a stray command cannot start pulling the
whole fleet.

#### Choosing the time range

`--start`/`-S` and `--end`/`-E` take a timestamp (`2026-09-01`,
`2026-09-01T12:00:00`, with an optional `Z` or `+02:00` offset), `now`, or an
offset from now such as `-90m`. `--duration`/`-d` takes a length: `30s`, `10m`,
`1h30m`, `2d`, `1w`, or a bare number of seconds. All times are UTC.

Any two of the three fix the window; a lone `--start` runs up to now, and a lone
`--duration` is the window ending now:

```shell
sqcli waveforms -u A3B7K9Q2 -S 2026-09-01 -E 2026-09-02 -o day.mseed
sqcli waveforms -u A3B7K9Q2 -S 2026-09-01T06:00:00 -d 30m -o event.mseed
sqcli waveforms -u A3B7K9Q2 -d 5m -o now.mseed
```

Long windows are fetched in day-sized requests, cut at UTC midnight; `--chunk`
changes that length.

#### Choosing the output

`--output`/`-o` decides where the miniSEED goes:

- a **file** — every record in one file, e.g. `-o event.mseed`.
- a **directory** — an [SDS] archive, used when the path names an existing
  directory or ends in a `/`, e.g. `-o archive/`.
- `-` — standard output, for piping into another tool.

An SDS archive stores one file per stream and per day:

```text
<archive>/<year>/<net>/<sta>/<chan>.D/<net>.<sta>.<loc>.<chan>.D.<year>.<doy>
```

```shell
sqcli waveforms -f online -S 2026-09-01 -E 2026-09-08 -o archive/
```

```text
archive/2026/QS/A3B7K9Q2/HHZ.D/QS.A3B7K9Q2..HHZ.D.2026.244
archive/2026/QS/A3B7K9Q2/HHZ.D/QS.A3B7K9Q2..HHZ.D.2026.245
```

Records are appended, so a download extends an existing archive rather than
replacing it — the same window fetched twice is stored twice. A single output
file is overwritten instead.

Once written, an archive can be read by any SDS-aware tool, e.g. SeisComP's
`scart`, or ObsPy's `obspy.clients.filesystem.sds.Client`.

#### Request options

- `--quality` — SEED data quality: `b` (best available, the default), `d`, `r`,
  `q` or `m`.
- `--minimum-length` — drop segments shorter than this many seconds.
- `--longest-only` — keep only the longest segment per stream.

[SDS]: https://www.seiscomp.de/doc/base/glossary.html#term-SDS

### `tui <sensor>`

Watch a sensor's waveforms live in the terminal, one stacked panel per channel.
This is the live counterpart to `waveforms`: that fetches a window that has
already passed, this follows one as it happens.

```shell
sqcli tui A3B7K9Q2
```

```text
A3B7K9Q2 · websocket  live  A3B7K9Q2  backend websocket  ·  201 packets  ·  0.4s behind
┌ EHZ  100 Hz  ±41.8k ─────────────────────────────────────────────────────┐
│                     ⢠⡀                                                   │
│    ⢰⣼⡀⣾⡀⣾ ⣾ ⡇⢸⡆⢸⡆⣸⡆⣸⣆⣧⢠⣧⢸⣿⢰⡇⢰⡇⣾ ⣿ ⣷ ⣷ ⣷⢀⡇⣸⡆⣸⡄⣼⡄⣼⡀⣾⡀⣾ ⣾ ⣷ ⣷ ⢰⡇⣸⡆⣼⡄⣾⡀│
│    ⢸⡇⢿⠁⢿⠁⢿ ⢿⢸⡇⢸⠇⢹⠇⢹⠘⡟⠘⡟⢸⣼⠸⡇⠸⡇⢸ ⢿ ⣿ ⡿ ⡿⢹⠇⢹⠃⢻⠃⢿⠁⢿⠁⢿ ⢿ ⡿ ⡿ ⢹⠇⢻⠃⢿⠁⢿│
│                      ⠃                                                   │
└──────────────────────────────────────────────────────────────────────────┘
```

#### Which sensor

The argument is either a **sensor UID** or the **address of a sensor on your
network**:

```shell
sqcli tui A3B7K9Q2          # through the backend
sqcli tui 192.168.178.55    # straight from the sensor on your LAN
sqcli tui qssensor.local    # the same, by name
```

Sensor UIDs are letters and digits, so anything containing a dot or a colon is
read as an address. `sqcli detect` prints the addresses to use, and an address
may carry its own port (`192.168.178.55:18010`).

#### Which route the data takes

Three routes carry the same waveforms. Unless you say otherwise, `sqcli` uses
the **websocket** through the backend — the same path the web interface takes.

```shell
sqcli tui A3B7K9Q2              # websocket, the default
sqcli tui A3B7K9Q2 --seedlink   # through the SeedLink relay instead
sqcli tui A3B7K9Q2 --fdsn       # by polling the archive instead
```

| Route | Needs | Good for |
| --- | --- | --- |
| websocket *(default)* | your account | anything your account can read, from anywhere |
| `--seedlink` | your IP registered, or a LAN sensor | standard protocol, lowest latency, no account for a LAN sensor |
| `--fdsn` | your account | a sensor that is not streaming; runs behind the archive |

**websocket** asks the sensor to start streaming and keeps asking while the view
is open, then receives the frames the backend relays. A sensor stops streaming a
minute after it was last asked, so closing the view stops it by itself.

**`--seedlink`** speaks [SeedLink], the usual seismological streaming protocol.
For a UID it goes through the relay at `seedlink.network.quakesaver.net`, which
decides what to hand out from the address you connect from — register your
public IP under *Waveforms > Network SeedLink Server*, or `sqcli` will report
that the relay serves it no sensors. `--server` and `--port` point it elsewhere.

**`--fdsn`** asks the same FDSN service `waveforms` downloads from, over and
over. It needs nothing of the sensor, so it works for one that is not streaming
at all, but it only ever shows what the archive has already stored.

A sensor named by **address** is always read straight from it over SeedLink: the
backend reaches sensors by UID, so it has no way to a machine on your network.
That sensor serves anyone who can reach it, but only once its own server is
running — start it under *Waveform access > SeedLink Server*.

The `--seedlink` and `--fdsn` routes are exclusive; the header names whichever
one is in use.

#### Keys

| Key | Effect |
| --- | --- |
| `q`, `Esc` | close the view |
| `↑` `↓` | move the highlight between channels |
| `⏎` | give the highlighted channel the whole window, or hand it back |
| `+` `-` | show more or less time (5 s to 10 min) |
| `a` | scale every channel alike, for comparing components, or each to itself |

Each panel is labelled with its channel, its sample rate and the amplitude it is
scaled to, and each column of a panel keeps the highest and lowest sample that
falls in it, so a spike between two columns is drawn rather than skipped. The
time axis runs backwards from the newest sample that has arrived, and the header
counts how far behind the wall clock that is.

The connection re-dials itself if it drops, so leaving the view open through a
sensor reboot or a flaky link is fine.

[SeedLink]: https://docs.seismiq.net/features/seedlink.html

### `action <action> <sensor-uid>`

Send a remote command to a single sensor (identified by its UID) through the
cloud API. Requires authentication. Available actions:

- `reboot` — reboot the sensor.
- `blink` — blink the sensor's LED, handy for physically locating a unit.
- `check-update` — tell the sensor to check for a firmware update.

```shell
sqcli action blink A3B7K9Q2
```
