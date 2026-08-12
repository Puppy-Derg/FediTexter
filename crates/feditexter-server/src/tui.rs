//! A btop++-style live dashboard for the running server.
//!
//! Launch with `--tui`: a ratatui dashboard shows system CPU/memory meters with
//! btop-style green→yellow→red gradients, per-core bars, WebSocket connection
//! history, message throughput, voice channel occupancy, and a scrollable log
//! fed by both the chat hub and the server's own tracing output.
//!
//! `q` / `Esc` quits (and stops the server); `↑`/`↓`/`PgUp`/`PgDn` scroll the
//! log panel.

use std::collections::{HashMap, VecDeque};
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Gauge, List, ListItem, Paragraph, Sparkline};
use ratatui::Frame;

use sysinfo::{Pid, ProcessesToUpdate, System};

use crate::chat::HubEvent;
use crate::db::AppState;

/// A `std::io::Write` sink that appends whole lines to a shared ring buffer —
/// used as the tracing writer in TUI mode so logs appear inside the dashboard
/// instead of clobbering the terminal.
pub struct RingLog {
    buf: Arc<Mutex<VecDeque<String>>>,
    line: Vec<u8>,
    max: usize,
}

impl RingLog {
    pub fn new(buf: Arc<Mutex<VecDeque<String>>>, max: usize) -> Self {
        Self { buf, line: Vec::new(), max }
    }
}

impl Write for RingLog {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.line.extend_from_slice(data);
        while let Some(pos) = self.line.iter().position(|&b| b == b'\n') {
            let raw: Vec<u8> = self.line.drain(..=pos).collect();
            let s = String::from_utf8_lossy(&raw).trim_end().to_string();
            if !s.is_empty() {
                let mut buf = self.buf.lock().unwrap();
                buf.push_back(s);
                while buf.len() > self.max {
                    buf.pop_front();
                }
            }
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// How an event-log line should be colored.
#[derive(Clone, Copy, PartialEq, Eq)]
enum LogKind {
    Info,
    Warn,
    Error,
    Message,
    Presence,
    Voice,
    Typing,
}

impl LogKind {
    fn color(self) -> Color {
        match self {
            LogKind::Info => Color::Rgb(120, 180, 220),
            LogKind::Warn => Color::Yellow,
            LogKind::Error => Color::Red,
            LogKind::Message => Color::White,
            LogKind::Presence => Color::Cyan,
            LogKind::Voice => Color::Magenta,
            LogKind::Typing => Color::DarkGray,
        }
    }
}

struct Tui {
    state: AppState,
    hub_rx: tokio::sync::broadcast::Receiver<HubEvent>,
    log_ring: Arc<Mutex<VecDeque<String>>>,
    sys: System,
    rt: tokio::runtime::Handle,
    started: Instant,

    total_messages: u64,
    msg_this_second: u64,
    msg_history: VecDeque<u64>,
    conn_history: VecDeque<u64>,
    last_sample: Instant,

    lines: VecDeque<(LogKind, String)>,
    scroll_from_bottom: usize,
    channel_names: HashMap<u64, String>,
    last_name_refresh: Instant,
    last_proc_refresh: Instant,
}

/// Run the dashboard on the current thread. Returns when the user quits.
pub fn run_tui(
    state: AppState,
    log_ring: Arc<Mutex<VecDeque<String>>>,
    rt: tokio::runtime::Handle,
    quit_tx: tokio::sync::oneshot::Sender<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut terminal = ratatui::try_init()?;
    let hub_rx = state.hub.subscribe();
    let mut tui = Tui {
        state,
        hub_rx,
        log_ring,
        sys: System::new(),
        rt,
        started: Instant::now(),
        total_messages: 0,
        msg_this_second: 0,
        msg_history: VecDeque::with_capacity(120),
        conn_history: VecDeque::with_capacity(120),
        last_sample: Instant::now(),
        lines: VecDeque::with_capacity(4000),
        scroll_from_bottom: 0,
        channel_names: HashMap::new(),
        last_name_refresh: Instant::now() - Duration::from_secs(5),
        last_proc_refresh: Instant::now() - Duration::from_secs(5),
    };

    let result = tui.event_loop(&mut terminal);
    ratatui::restore();
    let _ = quit_tx.send(());
    result
}

impl Tui {
    fn event_loop(&mut self, terminal: &mut ratatui::DefaultTerminal) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            self.drain_events();
            self.sample();
            self.refresh_sysinfo();
            terminal.draw(|frame| self.render(frame))?;

            if crossterm::event::poll(Duration::from_millis(100))?
                && let crossterm::event::Event::Key(key) = crossterm::event::read()?
            {
                match key.code {
                    crossterm::event::KeyCode::Char('q') | crossterm::event::KeyCode::Esc => break,
                    crossterm::event::KeyCode::Up => self.scroll_from_bottom += 1,
                    crossterm::event::KeyCode::Down => {
                        self.scroll_from_bottom = self.scroll_from_bottom.saturating_sub(1)
                    }
                    crossterm::event::KeyCode::PageUp => self.scroll_from_bottom += 20,
                    crossterm::event::KeyCode::PageDown => {
                        self.scroll_from_bottom = self.scroll_from_bottom.saturating_sub(20)
                    }
                    _ => {}
                }
            }
            // Cap the redraw rate even if events arrive faster than the poll
            // timeout (e.g. under a pty that floods input).
            std::thread::sleep(Duration::from_millis(50));
        }
        Ok(())
    }

    fn drain_events(&mut self) {
        while let Ok(event) = self.hub_rx.try_recv() {
            self.on_hub_event(event);
        }
        let new_lines: Vec<String> = self.log_ring.lock().unwrap().drain(..).collect();
        for line in new_lines {
            let kind = if line.contains("ERROR") {
                LogKind::Error
            } else if line.contains("WARN") {
                LogKind::Warn
            } else {
                LogKind::Info
            };
            self.push_line(kind, line);
        }
    }

    fn on_hub_event(&mut self, event: HubEvent) {
        match event {
            HubEvent::Message { message } => {
                self.total_messages += 1;
                self.msg_this_second += 1;
                let body = message.body.replace('\n', " ");
                let text = if body.len() > 90 {
                    format!("{}…", &body[..90])
                } else {
                    body
                };
                self.push_line(LogKind::Message, format!("✉ conv#{} user#{}: {}", message.conversation_id, message.sender_id, text));
            }
            HubEvent::MessageEdited { message } => {
                self.push_line(LogKind::Message, format!("✎ edited user#{}: {}", message.sender_id, message.body));
            }
            HubEvent::MessageDeleted { conversation_id, message_id } => {
                self.push_line(LogKind::Message, format!("✕ deleted conv#{conversation_id} msg#{message_id}"));
            }
            HubEvent::Typing { from_username, .. } => {
                self.push_line(LogKind::Typing, format!("✎ {from_username} is typing"));
            }
            HubEvent::Presence { user_id, online } => {
                let state = if online { "online" } else { "offline" };
                self.push_line(LogKind::Presence, format!("◉ user#{user_id} {state}"));
            }
            HubEvent::Signal { .. } => {
                // Signaling is chattier than is useful on the dashboard.
            }
            HubEvent::VoicePresence { channel_id, username, joined, .. } => {
                let action = if joined { "joined" } else { "left" };
                self.push_line(LogKind::Voice, format!("🔊 {username} {action} voice #{channel_id}"));
            }
            HubEvent::VoiceState { .. } => {}
        }
    }

    fn push_line(&mut self, kind: LogKind, text: String) {
        self.lines.push_back((kind, text));
        while self.lines.len() > 2000 {
            self.lines.pop_front();
        }
    }

    fn sample(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.last_sample) >= Duration::from_secs(1) {
            let conns = self.state.presence.lock().unwrap().len() as u64;
            self.conn_history.push_back(conns);
            if self.conn_history.len() > 120 {
                self.conn_history.pop_front();
            }
            self.msg_history.push_back(self.msg_this_second);
            if self.msg_history.len() > 120 {
                self.msg_history.pop_front();
            }
            self.msg_this_second = 0;
            self.last_sample = now;
        }
        // Refresh channel names for the voice panel every few seconds.
        if now.duration_since(self.last_name_refresh) >= Duration::from_secs(2) {
            self.refresh_channel_names();
            self.last_name_refresh = now;
        }
    }

    fn refresh_channel_names(&mut self) {
        let channel_ids: Vec<u64> = {
            let voice = self.state.voice.lock().unwrap();
            voice.keys().map(|(_, c)| *c).collect()
        };
        let rt = self.rt.clone();
        let pool = self.state.pool.clone();
        let ids = channel_ids.clone();
        let names = rt.block_on(async move {
            let mut out = HashMap::new();
            for id in ids {
                let name: Option<String> = sqlx::query_scalar("SELECT name FROM conversations WHERE id = ?")
                    .bind(id)
                    .fetch_optional(&pool)
                    .await
                    .unwrap_or_default();
                if let Some(n) = name {
                    out.insert(id, n);
                }
            }
            out
        });
        for (id, name) in names {
            self.channel_names.insert(id, name);
        }
    }

    fn refresh_sysinfo(&mut self) {
        self.sys.refresh_cpu_usage();
        self.sys.refresh_memory();
        if self.last_proc_refresh.elapsed() >= Duration::from_secs(2) {
            let _ = self.sys.refresh_processes(ProcessesToUpdate::All, true);
            self.last_proc_refresh = Instant::now();
        }
    }

    fn uptime(&self) -> String {
        let s = self.started.elapsed().as_secs();
        format!("{:02}:{:02}:{:02}", s / 3600, (s % 3600) / 60, s % 60)
    }

    // ------------------------------------------------------------- rendering

    fn render(&self, frame: &mut Frame) {
        let area = frame.area();
        let chunks = Layout::vertical([
            Constraint::Length(3),
            Constraint::Length(5),
            Constraint::Min(6),
            Constraint::Min(8),
        ])
        .split(area);

        self.render_header(frame, chunks[0]);
        self.render_meters(frame, chunks[1]);
        self.render_history(frame, chunks[2]);
        self.render_log(frame, chunks[3]);
    }

    fn render_header(&self, frame: &mut Frame, area: ratatui::layout::Rect) {
        let conns = self.state.presence.lock().unwrap().len();
        let block = Block::default()
            .borders(ratatui::widgets::Borders::BOTTOM)
            .border_style(Style::new().fg(Color::Rgb(80, 160, 255)));
        let uptime = self.uptime();
        let version = env!("CARGO_PKG_VERSION");
        let line = Line::from(vec![
            Span::styled(
                " FediTexter server ",
                Style::new().fg(Color::Black).bg(Color::Cyan).add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled("v", Style::new().fg(Color::DarkGray)),
            Span::styled(version, Style::new().fg(Color::White)),
            Span::raw("   "),
            Span::styled("uptime", Style::new().fg(Color::DarkGray)),
            Span::styled(uptime, Style::new().fg(Color::Green)),
            Span::raw("   "),
            Span::styled("users", Style::new().fg(Color::DarkGray)),
            Span::styled(conns.to_string(), Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
            Span::raw("   "),
            Span::styled("msgs", Style::new().fg(Color::DarkGray)),
            Span::styled(self.total_messages.to_string(), Style::new().fg(Color::White)),
            Span::raw("   "),
            Span::styled("logs", Style::new().fg(Color::DarkGray)),
            Span::styled(self.lines.len().to_string(), Style::new().fg(Color::Yellow)),
            Span::raw("   "),
            Span::styled("q quit", Style::new().fg(Color::DarkGray)),
        ]);
        frame.render_widget(Paragraph::new(line).block(block), area);
    }

    fn render_meters(&self, frame: &mut Frame, area: ratatui::layout::Rect) {
        let chunks = Layout::horizontal([
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ])
        .split(area);

        let cpu = self.sys.global_cpu_usage() / 100.0;
        self.gauge(frame, chunks[0], "CPU", cpu, format!("{:.1}%", cpu * 100.0));

        let total = self.sys.total_memory();
        let used = self.sys.used_memory();
        let mem_frac = if total > 0 { used as f32 / total as f32 } else { 0.0 };        self.gauge(
            frame,
            chunks[1],
            "Memory",
            mem_frac,
            format!("{} / {}", human_bytes(used), human_bytes(total)),
        );

        let (proc_frac, proc_label) = self.process_meter();
        self.gauge(frame, chunks[2], "Server proc", proc_frac, proc_label);

        let msg_rate = self.msg_history.back().copied().unwrap_or(0);
        self.gauge(
            frame,
            chunks[3],
            "Msg / s",
            (msg_rate as f32 / 200.0).min(1.0),
            format!("{msg_rate}/s  ·  {}", self.total_messages),
        );
    }

    fn process_meter(&self) -> (f32, String) {
        let total = self.sys.total_memory();
        let pid = Pid::from_u32(std::process::id());
        match self.sys.process(pid) {
            Some(proc) => {
                let mem = proc.memory();
                let frac = if total > 0 { mem as f32 / total as f32 } else { 0.0 };
                (frac, format!("rss {}", human_bytes(mem)))
            }
            None => (0.0, "n/a".into()),
        }
    }

    fn gauge(
        &self,
        frame: &mut Frame,
        area: ratatui::layout::Rect,
        title: &str,
        frac: f32,
        label: String,
    ) {
        let block = Block::bordered()
            .border_type(BorderType::Rounded)
            .border_style(Style::new().fg(Color::Rgb(90, 90, 110)))
            .title(Span::styled(title, Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD)));
        let g = Gauge::default()
            .block(block)
            .gauge_style(Style::new().fg(gradient(frac)).bg(Color::Rgb(40, 40, 50)))
            .ratio(frac.clamp(0.0, 1.0) as f64)
            .label(label);
        frame.render_widget(g, area);
    }

    fn render_history(&self, frame: &mut Frame, area: ratatui::layout::Rect) {
        let chunks = Layout::horizontal([
            Constraint::Percentage(30),
            Constraint::Percentage(30),
            Constraint::Percentage(40),
        ])
        .split(area);

        let conn_block = Block::bordered()
            .border_type(BorderType::Rounded)
            .border_style(Style::new().fg(Color::Rgb(90, 90, 110)))
            .title(Span::styled("Connections", Style::new().fg(Color::Green).add_modifier(Modifier::BOLD)));
        let conn_data: Vec<u64> = self.conn_history.iter().copied().collect();
        frame.render_widget(
            Sparkline::default()
                .block(conn_block)
                .data(&conn_data)
                .style(Style::new().fg(Color::Green)),
            chunks[0],
        );

        let msg_block = Block::bordered()
            .border_type(BorderType::Rounded)
            .border_style(Style::new().fg(Color::Rgb(90, 90, 110)))
            .title(Span::styled("Msgs / sec", Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD)));
        let msg_data: Vec<u64> = self.msg_history.iter().copied().collect();
        frame.render_widget(
            Sparkline::default()
                .block(msg_block)
                .data(&msg_data)
                .style(Style::new().fg(Color::Cyan)),
            chunks[1],
        );

        let voice = self.state.voice.lock().unwrap();
        let mut items: Vec<ListItem> = Vec::new();
        for ((_guild, channel), occupants) in voice.iter() {
            let name = self
                .channel_names
                .get(channel)
                .cloned()
                .unwrap_or_else(|| format!("#{channel}"));
            let mut users: Vec<String> = occupants.values().cloned().collect();
            users.sort();
            let line = Line::from(vec![
                Span::styled("🔊 ", Style::new().fg(Color::Magenta)),
                Span::styled(name, Style::new().fg(Color::White)),
                Span::raw("  "),
                Span::styled(users.join(", "), Style::new().fg(Color::Magenta)),
            ]);
            items.push(ListItem::new(line));
        }
        if items.is_empty() {
            items.push(ListItem::new(
                Line::from(Span::styled("no active voice channels", Style::new().fg(Color::DarkGray))),
            ));
        }
        let list_block = Block::bordered()
            .border_type(BorderType::Rounded)
            .border_style(Style::new().fg(Color::Rgb(90, 90, 110)))
            .title(Span::styled("Voice channels", Style::new().fg(Color::Magenta).add_modifier(Modifier::BOLD)));
        frame.render_widget(List::new(items).block(list_block), chunks[2]);
    }

    fn render_log(&self, frame: &mut Frame, area: ratatui::layout::Rect) {
        let block = Block::bordered()
            .border_type(BorderType::Rounded)
            .border_style(Style::new().fg(Color::Rgb(90, 90, 110)))
            .title(Span::styled("Activity", Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD)));
        let inner = block.inner(area);

        let inner_height = inner.height as usize;
        let total = self.lines.len();
        let start = total.saturating_sub(inner_height + self.scroll_from_bottom);
        let mut lines: Vec<Line> = Vec::new();
        for (kind, text) in self.lines.iter().skip(start).take(inner_height) {
            lines.push(Line::from(Span::styled(text.as_str(), Style::new().fg(kind.color()))));
        }
        frame.render_widget(Paragraph::new(lines).block(block), area);
    }
}

fn gradient(frac: f32) -> Color {
    // btop-style green -> yellow -> red.
    let f = frac.clamp(0.0, 1.0);
    let (r, g) = if f < 0.5 {
        ((f * 2.0 * 255.0) as u8, 255)
    } else {
        (255, ((2.0 - f * 2.0) * 255.0) as u8)
    };
    Color::Rgb(r, g, 0)
}

fn human_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    let b = bytes as f64;
    if b >= KIB * KIB * KIB {
        format!("{:.1} GiB", b / (KIB * KIB * KIB))
    } else if b >= KIB * KIB {
        format!("{:.1} MiB", b / (KIB * KIB))
    } else if b >= KIB {
        format!("{:.1} KiB", b / KIB)
    } else {
        format!("{b} B")
    }
}
