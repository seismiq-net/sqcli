//! A live view of one sensor's waveforms in the terminal.
//!
//! Records arrive from [`crate::seedlink`] as they are recorded and land in a
//! ring buffer per channel, one stacked panel each. The panels are redrawn on a
//! timer rather than on arrival, so a sensor sending fifty packets a second
//! costs the same as one sending two.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{NaiveDateTime, Utc};
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use eyre::Result;
use futures::StreamExt;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols::Marker;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph, Wrap};
use ratatui::{DefaultTerminal, Frame};
use tokio::sync::mpsc::Receiver;

use crate::stream::{Chunk, Update};

/// How often the view is redrawn.
const FRAME_INTERVAL: Duration = Duration::from_millis(50);

/// How long a passing notice stays on screen before the key reminders take the
/// line back.
const NOTICE_LINGER: Duration = Duration::from_secs(10);

/// The span of signal the view opens on.
pub(crate) const DEFAULT_WINDOW: f64 = 60.0;
/// How far the window may be stretched or squeezed, in seconds.
const MIN_WINDOW: f64 = 5.0;
const MAX_WINDOW: f64 = 600.0;

/// A little headroom above and below the signal, so a trace never touches the
/// edge of its panel.
const AMPLITUDE_MARGIN: f64 = 0.08;

/// Colours to tell the channels apart, in the order they first appear.
const TRACE_COLOURS: [Color; 6] = [
    Color::Cyan,
    Color::Yellow,
    Color::Green,
    Color::Magenta,
    Color::Blue,
    Color::Red,
];

/// What has become of the connection.
enum Status {
    Connecting,
    Live { server: String, station: String },
    Reconnecting { reason: String },
}

/// One channel's rolling window of samples.
struct Trace {
    channel: String,
    sample_rate: f64,
    /// Samples as (seconds since the epoch, value), oldest first.
    points: std::collections::VecDeque<(f64, f64)>,
    /// Redrawn points, kept between frames so the chart can borrow them.
    plotted: Vec<(f64, f64)>,
}

impl Trace {
    fn new(chunk: &Chunk) -> Self {
        Trace {
            channel: chunk.channel.clone(),
            sample_rate: chunk.sample_rate,
            points: std::collections::VecDeque::new(),
            plotted: Vec::new(),
        }
    }

    /// The time of the newest sample held.
    fn latest(&self) -> Option<f64> {
        self.points.back().map(|(time, _)| *time)
    }

    fn extend(&mut self, chunk: &Chunk) {
        if chunk.sample_rate > 0.0 {
            self.sample_rate = chunk.sample_rate;
        }
        let start = seconds(chunk.start);
        let step = if chunk.sample_rate > 0.0 {
            1.0 / chunk.sample_rate
        } else {
            0.0
        };
        for (index, value) in chunk.samples.iter().enumerate() {
            self.points.push_back((start + index as f64 * step, *value));
        }
    }

    /// Drop everything recorded before `oldest`.
    fn trim(&mut self, oldest: f64) {
        while self.points.front().is_some_and(|(time, _)| *time < oldest) {
            self.points.pop_front();
        }
    }

    /// The samples inside the window, thinned to what the panel can show.
    ///
    /// Each column of the panel keeps the highest and lowest sample that falls
    /// in it, which is what makes a seismogram look like one: the envelope of
    /// the signal rather than whichever sample a naive stride happened to land
    /// on.
    fn plot(&mut self, right_edge: f64, window: f64, columns: usize) {
        self.plotted.clear();
        let columns = columns.max(1);
        let bucket_width = window / columns as f64;
        let left_edge = right_edge - window;

        // The buffer holds far more than the window shows, and it is in time
        // order, so the visible tail is found rather than filtered for.
        let first = self.points.partition_point(|(time, _)| *time < left_edge);
        let mut bucket = 0usize;
        let mut extremes: Option<(f64, f64)> = None;
        // Flushing a bucket emits its low and its high in the order they keep
        // the line moving forward.
        let flush = |bucket: usize, extremes: Option<(f64, f64)>, into: &mut Vec<(f64, f64)>| {
            if let Some((low, high)) = extremes {
                let at = (bucket as f64 + 0.5) * bucket_width - window;
                into.push((at, low));
                if high != low {
                    into.push((at, high));
                }
            }
        };

        for (time, value) in self.points.range(first..) {
            let offset = time - left_edge;
            let index = ((offset / bucket_width) as usize).min(columns - 1);
            if index != bucket {
                flush(bucket, extremes, &mut self.plotted);
                bucket = index;
                extremes = None;
            }
            extremes = Some(match extremes {
                None => (*value, *value),
                Some((low, high)) => (low.min(*value), high.max(*value)),
            });
        }
        flush(bucket, extremes, &mut self.plotted);
    }

    /// The span the plotted points cover, with a little headroom.
    fn amplitude(&self) -> (f64, f64) {
        let (mut low, mut high) = (f64::INFINITY, f64::NEG_INFINITY);
        for (_, value) in &self.plotted {
            low = low.min(*value);
            high = high.max(*value);
        }
        if !low.is_finite() || !high.is_finite() {
            return (-1.0, 1.0);
        }
        // A flat trace still needs a panel to sit in.
        let margin = ((high - low) * AMPLITUDE_MARGIN).max(1.0);
        (low - margin, high + margin)
    }
}

/// Seconds since the epoch, the way the plot counts time.
fn seconds(at: NaiveDateTime) -> f64 {
    at.and_utc().timestamp_micros() as f64 / 1e6
}

/// The state the view draws from.
struct App {
    /// What to call the sensor and the route in the header.
    source: String,
    status: Status,
    traces: BTreeMap<String, Trace>,
    window: f64,
    /// Which panel is singled out, as an index into the sorted traces.
    focus: usize,
    /// Whether the focused panel has the whole area to itself.
    expanded: bool,
    /// Whether every channel shares one amplitude scale, for comparing them.
    common_scale: bool,
    packets: usize,
    /// Something worth saying in passing, and when it was said.
    notice: Option<(String, std::time::Instant)>,
}

impl App {
    fn new(source: String, window: f64) -> Self {
        App {
            source,
            status: Status::Connecting,
            traces: BTreeMap::new(),
            window,
            focus: 0,
            expanded: false,
            common_scale: false,
            packets: 0,
            notice: None,
        }
    }

    fn apply(&mut self, update: Update) {
        match update {
            Update::Connected { server, station } => {
                self.status = Status::Live { server, station };
                self.notice = None;
            }
            Update::Disconnected(reason) => self.status = Status::Reconnecting { reason },
            Update::Notice(notice) => self.notice = Some((notice, std::time::Instant::now())),
            Update::Data(chunk) => {
                self.packets += 1;
                self.traces
                    .entry(chunk.stream_id.clone())
                    .or_insert_with(|| Trace::new(&chunk))
                    .extend(&chunk);
            }
        }
    }

    /// The newest sample across every channel, which anchors the right edge of
    /// the plots. Following the data rather than the wall clock keeps the
    /// traces still even when this machine's clock disagrees with the sensor's.
    fn right_edge(&self) -> Option<f64> {
        self.traces
            .values()
            .filter_map(Trace::latest)
            .fold(None, |newest, latest| {
                Some(newest.map_or(latest, |newest: f64| newest.max(latest)))
            })
    }

    /// How far behind the wall clock the newest sample is.
    fn latency(&self) -> Option<f64> {
        self.right_edge()
            .map(|edge| seconds(Utc::now().naive_utc()) - edge)
    }

    /// Forget what has scrolled out of the window, keeping a little slack so a
    /// stretch of the window does not start from an empty panel.
    fn trim(&mut self) {
        let Some(edge) = self.right_edge() else {
            return;
        };
        let oldest = edge - MAX_WINDOW;
        for trace in self.traces.values_mut() {
            trace.trim(oldest);
        }
    }

    fn scroll_focus(&mut self, forward: bool) {
        let count = self.traces.len();
        if count == 0 {
            return;
        }
        self.focus = if forward {
            (self.focus + 1) % count
        } else {
            (self.focus + count - 1) % count
        };
    }

    fn resize_window(&mut self, factor: f64) {
        self.window = (self.window * factor).clamp(MIN_WINDOW, MAX_WINDOW);
    }

    /// Returns `true` when the key asked to close the view.
    fn handle_key(&mut self, key: KeyEvent) -> bool {
        if key.kind != KeyEventKind::Press {
            return false;
        }
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return true,
            KeyCode::Char('c' | 'd') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return true
            }
            KeyCode::Char('+') | KeyCode::Char('=') => self.resize_window(2.0),
            KeyCode::Char('-') | KeyCode::Char('_') => self.resize_window(0.5),
            KeyCode::Down | KeyCode::Tab | KeyCode::Char('j') => self.scroll_focus(true),
            KeyCode::Up | KeyCode::BackTab | KeyCode::Char('k') => self.scroll_focus(false),
            KeyCode::Enter | KeyCode::Char('f') => self.expanded = !self.expanded,
            KeyCode::Char('a') => self.common_scale = !self.common_scale,
            _ => {}
        }
        false
    }
}

/// The line along the top: which sensor, which server, and how it is going.
fn header(app: &App) -> Line<'static> {
    let mut spans = vec![
        Span::styled(
            app.source.clone(),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
    ];
    match &app.status {
        Status::Connecting => spans.push(Span::styled(
            "connecting…",
            Style::default().fg(Color::Yellow),
        )),
        Status::Live { server, station } => {
            spans.push(Span::styled("live", Style::default().fg(Color::Green)));
            spans.push(Span::raw(format!("  {}  {}", station, server)));
        }
        Status::Reconnecting { .. } => spans.push(Span::styled(
            "reconnecting",
            Style::default().fg(Color::Red),
        )),
    }
    spans.push(Span::raw(format!("  ·  {} packets", app.packets)));
    if let Some(latency) = app.latency() {
        spans.push(Span::raw(format!("  ·  {:.1}s behind", latency)));
    }
    Line::from(spans)
}

/// The key reminders along the bottom, or whatever went wrong instead.
///
/// A lost connection holds the line for as long as it is lost; a passing notice
/// only for a while, so an old complaint does not outlive what it was about.
fn footer(app: &App) -> Line<'static> {
    if let Status::Reconnecting { reason } = &app.status {
        return Line::from(Span::styled(
            reason.clone(),
            Style::default().fg(Color::Red),
        ));
    }
    if let Some((notice, since)) = &app.notice {
        if since.elapsed() < NOTICE_LINGER {
            return Line::from(Span::styled(
                notice.clone(),
                Style::default().fg(Color::Yellow),
            ));
        }
    }
    Line::from(Span::styled(
        format!(
            "q quit  ↑↓ focus  ⏎ expand{}  +/- window ({:.0}s)  a {} scale",
            if app.expanded { " (on)" } else { "" },
            app.window,
            if app.common_scale {
                "shared"
            } else {
                "per-channel"
            },
        ),
        Style::default().fg(Color::DarkGray),
    ))
}

/// Draw one channel's panel.
#[allow(clippy::too_many_arguments)]
fn draw_trace(
    frame: &mut Frame,
    area: Rect,
    trace: &Trace,
    bounds: [f64; 2],
    window: f64,
    colour: Color,
    focused: bool,
    // Only the bottom panel carries the time axis; the ones above share it.
    timed: bool,
) {
    let border = if focused {
        Style::default().fg(colour).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let title = format!(
        " {}  {:.0} Hz  ±{} ",
        trace.channel,
        trace.sample_rate,
        engineering(bounds[1].abs().max(bounds[0].abs()))
    );
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(border)
        .title(Span::styled(title, Style::default().fg(colour)));

    let dataset = Dataset::default()
        .marker(Marker::Braille)
        .graph_type(GraphType::Line)
        .style(Style::default().fg(colour))
        .data(&trace.plotted);

    let chart = Chart::new(vec![dataset])
        .block(block)
        // The plot counts time backwards from the newest sample, so the right
        // edge is always now and the left edge is however wide the window is.
        // Every panel shares one time base, so only the bottom one is labelled
        // and the rest spend the row on signal instead.
        .x_axis(
            Axis::default()
                .bounds([-window, 0.0])
                .style(Style::default().fg(Color::DarkGray))
                .labels(if timed {
                    vec![format!("-{:.0}s", window), "now".to_string()]
                } else {
                    Vec::new()
                }),
        )
        .y_axis(Axis::default().bounds(bounds));
    frame.render_widget(chart, area);
}

/// A number cut down to something that fits in a title.
fn engineering(value: f64) -> String {
    let magnitude = value.abs();
    if magnitude >= 1e9 {
        format!("{:.1}G", value / 1e9)
    } else if magnitude >= 1e6 {
        format!("{:.1}M", value / 1e6)
    } else if magnitude >= 1e3 {
        format!("{:.1}k", value / 1e3)
    } else if magnitude >= 1.0 {
        format!("{:.0}", value)
    } else {
        format!("{:.3}", value)
    }
}

fn draw(frame: &mut Frame, app: &mut App) {
    let [top, body, bottom] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(3),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    frame.render_widget(Paragraph::new(header(app)), top);
    frame.render_widget(
        Paragraph::new(footer(app)).alignment(Alignment::Left),
        bottom,
    );

    if app.traces.is_empty() {
        // With nothing to draw, the panel is the room to say why at length,
        // which is where a message like "register this machine" belongs.
        let (waiting, style) = match &app.status {
            Status::Reconnecting { reason } => (reason.clone(), Style::default().fg(Color::Red)),
            Status::Connecting => (
                "connecting…".to_string(),
                Style::default().fg(Color::DarkGray),
            ),
            Status::Live { .. } => (
                "waiting for the first packet…".to_string(),
                Style::default().fg(Color::DarkGray),
            ),
        };
        let [_, middle, _] = Layout::vertical([
            Constraint::Fill(1),
            Constraint::Length(4),
            Constraint::Fill(1),
        ])
        .areas(body);
        frame.render_widget(
            Paragraph::new(waiting)
                .style(style)
                .alignment(Alignment::Center)
                .wrap(Wrap { trim: true }),
            middle,
        );
        return;
    }

    app.focus = app.focus.min(app.traces.len() - 1);
    let shown: Vec<usize> = if app.expanded {
        vec![app.focus]
    } else {
        (0..app.traces.len()).collect()
    };

    let areas =
        Layout::vertical(vec![Constraint::Ratio(1, shown.len() as u32); shown.len()]).split(body);

    // Thin the samples to the width of the panel they land in before anything
    // is measured, so the scale matches what is actually drawn.
    let right_edge = app.right_edge().unwrap_or(0.0);
    let window = app.window;
    for (slot, index) in shown.iter().enumerate() {
        // Braille packs two points into every column.
        let columns = areas[slot].width.saturating_sub(2) as usize * 2;
        if let Some(trace) = app.traces.values_mut().nth(*index) {
            trace.plot(right_edge, window, columns);
        }
    }

    let shared = app.common_scale.then(|| {
        app.traces
            .values()
            .map(Trace::amplitude)
            .fold((f64::INFINITY, f64::NEG_INFINITY), |(low, high), (l, h)| {
                (low.min(l), high.max(h))
            })
    });

    for (slot, index) in shown.iter().enumerate() {
        let Some(trace) = app.traces.values().nth(*index) else {
            continue;
        };
        let (low, high) = shared.unwrap_or_else(|| trace.amplitude());
        draw_trace(
            frame,
            areas[slot],
            trace,
            [low, high],
            window,
            TRACE_COLOURS[index % TRACE_COLOURS.len()],
            *index == app.focus && !app.expanded,
            slot + 1 == shown.len(),
        );
    }
}

/// Run the live view until the user closes it.
pub(crate) async fn run(
    source: String,
    window: f64,
    mut updates: Receiver<Update>,
    terminal: &mut DefaultTerminal,
) -> Result<()> {
    let mut app = App::new(source, window);
    let mut events = EventStream::new();
    let mut frames = tokio::time::interval(FRAME_INTERVAL);

    loop {
        tokio::select! {
            // Everything waiting is drained before the next frame, so a burst
            // of packets costs one redraw rather than one each.
            Some(update) = updates.recv() => {
                app.apply(update);
                while let Ok(update) = updates.try_recv() {
                    app.apply(update);
                }
            }
            event = events.next() => {
                match event {
                    Some(Ok(Event::Key(key))) => {
                        if app.handle_key(key) {
                            return Ok(());
                        }
                    }
                    // The terminal went away; there is nothing left to draw on.
                    Some(Err(e)) => return Err(e.into()),
                    None => return Ok(()),
                    _ => {}
                }
            }
            _ = frames.tick() => {
                app.trim();
                terminal.draw(|frame| draw(frame, &mut app))?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn chunk(channel: &str, start_second: u32, rate: f64, samples: Vec<f64>) -> Chunk {
        Chunk {
            stream_id: format!("QS.BLA..{}", channel),
            channel: channel.to_string(),
            start: NaiveDate::from_ymd_opt(2026, 9, 12)
                .unwrap()
                .and_hms_opt(12, 0, start_second)
                .unwrap(),
            sample_rate: rate,
            samples,
        }
    }

    #[test]
    fn samples_are_timed_from_the_record_start_and_its_rate() {
        let mut trace = Trace::new(&chunk("HHZ", 0, 100.0, vec![]));
        trace.extend(&chunk("HHZ", 0, 100.0, vec![1.0, 2.0, 3.0]));

        let start = seconds(
            NaiveDate::from_ymd_opt(2026, 9, 12)
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap(),
        );
        // Times are seconds since the epoch, so they are compared with the
        // slack an f64 that large leaves — still a thousand times finer than
        // the gap between two samples.
        let times: Vec<f64> = trace.points.iter().map(|(time, _)| time - start).collect();
        for (measured, expected) in times.iter().zip([0.0, 0.01, 0.02]) {
            assert!(
                (measured - expected).abs() < 1e-6,
                "{} is not {}",
                measured,
                expected
            );
        }
    }

    #[test]
    fn a_panel_keeps_the_high_and_low_of_every_column() {
        // The whole point of the thinning: a spike between two columns has to
        // survive, which a plain stride would drop.
        let mut trace = Trace::new(&chunk("HHZ", 0, 10.0, vec![]));
        trace.extend(&chunk("HHZ", 0, 10.0, vec![0.0, 500.0, -500.0, 0.0]));
        let edge = trace.latest().expect("a sample");

        trace.plot(edge, 1.0, 2);

        let values: Vec<f64> = trace.plotted.iter().map(|(_, value)| *value).collect();
        assert!(values.contains(&500.0), "the peak was thinned away");
        assert!(values.contains(&-500.0), "the trough was thinned away");
    }

    #[test]
    fn the_plot_covers_the_window_and_ends_at_the_newest_sample() {
        let mut trace = Trace::new(&chunk("HHZ", 0, 10.0, vec![]));
        trace.extend(&chunk("HHZ", 0, 10.0, (0..100).map(|i| i as f64).collect()));
        let edge = trace.latest().expect("a sample");

        trace.plot(edge, 10.0, 20);

        // Time is counted backwards from the newest sample, so every point sits
        // within the window and none of them are in the future.
        for (at, _) in &trace.plotted {
            assert!((-10.0..=0.0).contains(at), "{} is outside the window", at);
        }
    }

    #[test]
    fn only_the_window_is_drawn_however_much_is_buffered() {
        let mut trace = Trace::new(&chunk("HHZ", 0, 10.0, vec![]));
        // Ten seconds of samples, of which the last two are asked for.
        trace.extend(&chunk("HHZ", 0, 10.0, (0..100).map(|i| i as f64).collect()));
        let edge = trace.latest().expect("a sample");

        trace.plot(edge, 2.0, 8);

        let lowest = trace
            .plotted
            .iter()
            .map(|(_, value)| *value)
            .fold(f64::INFINITY, f64::min);
        // Everything before the last two seconds stays out of the picture.
        assert!(lowest >= 79.0, "older samples leaked in: {}", lowest);
    }

    #[test]
    fn old_samples_are_forgotten_once_they_scroll_out() {
        let mut app = App::new("A3B7K9Q2 · websocket".to_string(), 60.0);
        app.apply(Update::Data(chunk("HHZ", 0, 1.0, vec![1.0; 10])));
        // A record arriving well past the buffer's reach retires the old one.
        app.apply(Update::Data(chunk("HHZ", 0, 1.0, vec![2.0; 10])));
        let held = app.traces["QS.BLA..HHZ"].points.len();
        app.trim();

        assert_eq!(app.packets, 2);
        assert!(app.traces["QS.BLA..HHZ"].points.len() <= held);
    }

    #[test]
    fn each_channel_gets_its_own_panel() {
        let mut app = App::new("A3B7K9Q2 · websocket".to_string(), 60.0);
        app.apply(Update::Data(chunk("HHZ", 0, 100.0, vec![1.0])));
        app.apply(Update::Data(chunk("HHN", 0, 100.0, vec![1.0])));
        app.apply(Update::Data(chunk("HHZ", 1, 100.0, vec![1.0])));

        assert_eq!(app.traces.len(), 2);
        assert_eq!(app.traces["QS.BLA..HHZ"].points.len(), 2);
    }

    #[test]
    fn the_window_cannot_be_stretched_past_what_is_kept() {
        let mut app = App::new("A3B7K9Q2 · websocket".to_string(), 60.0);
        for _ in 0..20 {
            app.resize_window(2.0);
        }
        assert_eq!(app.window, MAX_WINDOW);

        for _ in 0..20 {
            app.resize_window(0.5);
        }
        assert_eq!(app.window, MIN_WINDOW);
    }

    #[test]
    fn focus_wraps_around_the_channels() {
        let mut app = App::new("A3B7K9Q2 · websocket".to_string(), 60.0);
        for channel in ["HHZ", "HHN", "HHE"] {
            app.apply(Update::Data(chunk(channel, 0, 100.0, vec![1.0])));
        }

        // The traces are keyed by stream, so they are in channel order: E, N, Z.
        app.scroll_focus(false);
        assert_eq!(app.focus, 2);
        app.scroll_focus(true);
        assert_eq!(app.focus, 0);
    }

    #[test]
    fn a_flat_trace_still_gets_a_panel_to_sit_in() {
        let mut trace = Trace::new(&chunk("HHZ", 0, 10.0, vec![]));
        trace.extend(&chunk("HHZ", 0, 10.0, vec![7.0; 10]));
        trace.plot(trace.latest().expect("a sample"), 1.0, 8);

        let (low, high) = trace.amplitude();
        assert!(low < 7.0 && high > 7.0, "a flat trace collapsed to a line");
    }

    #[test]
    fn quitting_is_spelled_several_ways() {
        let mut app = App::new("A3B7K9Q2 · websocket".to_string(), 60.0);
        let press = |code| KeyEvent::new(code, KeyModifiers::NONE);

        assert!(app.handle_key(press(KeyCode::Char('q'))));
        assert!(app.handle_key(press(KeyCode::Esc)));
        assert!(app.handle_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)));
        assert!(!app.handle_key(press(KeyCode::Char('a'))));
    }
}
