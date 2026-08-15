//! In-app media playback engine.
//!
//! ffmpeg-based decode of local video/audio files into RGBA frames for iced
//! image handles plus PCM for a cpal output stream. The decode loop lives on a
//! dedicated thread (ffmpeg types are `!Send`) and is driven by an owner via a
//! command channel; decoded frames flow out through a `std::sync::mpsc` channel
//! that the iced layer drains through a subscription.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};

use ffmpeg_next as ffmpeg;

pub const SAMPLE_RATE: u32 = 44_100;

// Keep at most ~1 second of stereo audio buffered so a decode burst can't
// balloon memory on a low-bitrate audio track.
const AUDIO_RING_MAX: usize = (SAMPLE_RATE * 2) as usize;

#[derive(Debug, Clone)]
pub enum MediaEvent {
    Opened { duration: f64, has_audio: bool },
    Frame { width: u32, height: u32, rgba: Arc<Vec<u8>> },
    Ended,
    Error(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlayState {
    Stopped,
    Playing,
    Paused,
}

impl PlayState {
    fn from_u8(v: u8) -> Self {
        match v {
            1 => PlayState::Playing,
            2 => PlayState::Paused,
            _ => PlayState::Stopped,
        }
    }
}

enum Cmd {
    Play,
    Pause,
    Seek(f64),
    Volume(f32),
    Stop,
}

enum CmdAction {
    Continue,
    Exit(SessionExit),
}

fn handle_cmd(
    cmd: Cmd,
    playing: &mut bool,
    pos: &mut f64,
    base: &mut std::time::Instant,
    volume: &std::sync::Arc<std::sync::atomic::AtomicU32>,
) -> CmdAction {
    match cmd {
        Cmd::Stop => CmdAction::Exit(SessionExit::Stop),
        Cmd::Seek(t) => CmdAction::Exit(SessionExit::Seek(t.max(0.0))),
        Cmd::Play => {
            if !*playing {
                *playing = true;
                *base = std::time::Instant::now();
            }
            CmdAction::Continue
        }
        Cmd::Pause => {
            if *playing {
                *pos += base.elapsed().as_secs_f64();
                *playing = false;
            }
            CmdAction::Continue
        }
        Cmd::Volume(v) => {
            volume.store(v.to_bits(), std::sync::atomic::Ordering::Relaxed);
            CmdAction::Continue
        }
    }
}

struct AudioRing {
    data: VecDeque<f32>,
}

/// Handle used by the UI thread to drive an open playback session.
pub struct MediaEngine {
    cmds: mpsc::Sender<Cmd>,
    events: Arc<Mutex<mpsc::Receiver<MediaEvent>>>,
    state: Arc<AtomicU8>,
    volume: Arc<AtomicU32>,
    position: Arc<AtomicU32>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl MediaEngine {
    pub fn open(path: PathBuf, seek: f64, volume: f32) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel::<Cmd>();
        let (ev_tx, ev_rx) = mpsc::channel::<MediaEvent>();
        let events = Arc::new(Mutex::new(ev_rx));
        let state = Arc::new(AtomicU8::new(1));
        let volume = Arc::new(AtomicU32::new(volume.to_bits()));
        let position = Arc::new(AtomicU32::new(0));
        let thread = std::thread::Builder::new()
            .name("media-decode".into())
            .spawn({
                let state = state.clone();
                let volume = volume.clone();
                let position = position.clone();
                move || {
                    run_loop(
                        path,
                        seek,
                        cmd_rx,
                        ev_tx,
                        state,
                        volume,
                        position,
                    )
                }
            })
            .ok();
        MediaEngine {
            cmds: cmd_tx,
            events,
            state,
            volume,
            position,
            thread,
        }
    }

    pub fn play(&self) {
        let _ = self.cmds.send(Cmd::Play);
    }

    pub fn pause(&self) {
        let _ = self.cmds.send(Cmd::Pause);
    }

    pub fn seek(&self, seconds: f64) {
        let _ = self.cmds.send(Cmd::Seek(seconds));
    }

    pub fn set_volume(&self, volume: f32) {
        let _ = self.cmds.send(Cmd::Volume(volume));
    }

    pub fn play_state(&self) -> PlayState {
        PlayState::from_u8(self.state.load(Ordering::Relaxed))
    }

    pub fn position_secs(&self) -> f32 {
        f32::from_bits(self.position.load(Ordering::Relaxed))
    }

    pub fn events(&self) -> Arc<Mutex<mpsc::Receiver<MediaEvent>>> {
        self.events.clone()
    }
}

impl Drop for MediaEngine {
    fn drop(&mut self) {
        let _ = self.cmds.send(Cmd::Stop);
    }
}

enum SessionExit {
    Seek(f64),
    Stop,
}

fn run_loop(
    path: PathBuf,
    mut seek: f64,
    cmd_rx: mpsc::Receiver<Cmd>,
    ev_tx: mpsc::Sender<MediaEvent>,
    state: Arc<AtomicU8>,
    volume: Arc<AtomicU32>,
    position: Arc<AtomicU32>,
) {
    loop {
        match run_session(&path, seek, &cmd_rx, &ev_tx, &state, &volume, &position) {
            Ok(SessionExit::Seek(t)) => {
                if std::env::var("FEDITEXTER_VIEWER_TRACE").is_ok() {
                    eprintln!("[media] run_loop restarting with seek={t}");
                }
                seek = t;
                state.store(1, Ordering::Relaxed);
                continue;
            }
            Ok(SessionExit::Stop) | Err(_) => return,
        }
    }
}

fn run_session(
    path: &PathBuf,
    seek: f64,
    cmd_rx: &mpsc::Receiver<Cmd>,
    ev_tx: &mpsc::Sender<MediaEvent>,
    state: &Arc<AtomicU8>,
    volume: &Arc<AtomicU32>,
    position: &Arc<AtomicU32>,
) -> Result<SessionExit, ()> {
    let _ = ffmpeg::init();

    if std::env::var("FEDITEXTER_MEDIA_TRACE").is_ok() {
        eprintln!("[media] init ok");
    }
    if std::env::var("FEDITEXTER_VIEWER_TRACE").is_ok() {
        eprintln!("[media] run_session enter seek={seek}");
    }

    let mut input = match ffmpeg::format::input(path) {
        Ok(i) => {
            if std::env::var("FEDITEXTER_MEDIA_TRACE").is_ok() {
                eprintln!("[media] input opened");
            }
            i
        }
        Err(e) => {
            if std::env::var("FEDITEXTER_MEDIA_TRACE").is_ok() {
                eprintln!("[media] input open error: {e}");
            }
            let _ = ev_tx.send(MediaEvent::Error(format!("open: {e}")));
            return Err(());
        }
    };

    // Real seek: jump the demuxer to the requested position instead of
    // decoding every frame from the first one (which blasted through the
    // whole preceding clip at decode speed — the "flashing" on scrub).
    // `avformat_seek_file` with stream index -1 works in AV_TIME_BASE (µs).
    // Done before taking the stream handles (Stream borrows the Input).
    if seek > 0.0 {
        let ts = (seek * 1_000_000.0) as i64;
        let r = input.seek(ts, 0..ts);
        if std::env::var("FEDITEXTER_VIEWER_TRACE").is_ok() {
            eprintln!("[media] input.seek({ts}) -> {r:?}");
        }
    }

    if std::env::var("FEDITEXTER_MEDIA_TRACE").is_ok() {
        eprintln!("[media] after input open, streams()");
    }

    let video = input.streams().best(ffmpeg::media::Type::Video);
    let audio = input.streams().best(ffmpeg::media::Type::Audio);
    let video_index = video.as_ref().map(|s| s.index());
    let audio_index = audio.as_ref().map(|s| s.index());

    let duration = input.duration() as f64 / 1_000_000.0;

    if std::env::var("FEDITEXTER_MEDIA_TRACE").is_ok() {
        eprintln!(
            "[media] video_index={:?} audio_index={:?} duration={duration}",
            video_index, audio_index
        );
    }

    let mut video_decoder = video
        .and_then(|s| ffmpeg::codec::context::Context::from_parameters(s.parameters()).ok())
        .and_then(|c| c.decoder().video().ok());

    if std::env::var("FEDITEXTER_MEDIA_TRACE").is_ok() {
        eprintln!("[media] video_decoder ready");
    }

    let mut scaler = None;
    if let Some(dec) = video_decoder.as_ref() {
        let w = dec.width();
        let h = dec.height();
        if w > 0 && h > 0 {
            scaler = ffmpeg::software::scaling::Context::get(
                dec.format(),
                w,
                h,
                ffmpeg::format::Pixel::RGBA,
                w,
                h,
                ffmpeg::software::scaling::flag::Flags::BILINEAR,
            )
            .ok();
        }
    }

    // Audio side.
    let mut audio_decoder = audio
        .and_then(|s| ffmpeg::codec::context::Context::from_parameters(s.parameters()).ok())
        .and_then(|c| c.decoder().audio().ok());

    // Report metadata before touching the audio device: audio init is
    // best-effort and must never hold up the first decoded frame.
    let has_audio = audio_decoder.is_some();
    if std::env::var("FEDITEXTER_MEDIA_TRACE").is_ok() {
        eprintln!("[media] sending Opened duration={duration} has_audio={has_audio}");
    }
    let _ = ev_tx.send(MediaEvent::Opened { duration, has_audio });

    // Audio output is opened on a separate thread so a slow/busy audio device
    // can never stall the video decode path. Silence is emitted on underrun.
    let ring = Arc::new(Mutex::new(AudioRing {
        data: VecDeque::with_capacity(AUDIO_RING_MAX),
    }));
    if std::env::var("FEDITEXTER_MEDIA_TRACE").is_ok() {
        eprintln!("[media] spawning output device");
    }
    let _audio = spawn_output(ring.clone(), volume.clone());
    if std::env::var("FEDITEXTER_MEDIA_TRACE").is_ok() {
        eprintln!("[media] output device spawned");
    }

    let mut resampler = None;
    if let Some(dec) = audio_decoder.as_mut() {
        let fmt = dec.format();
        let layout = dec.channel_layout();
        let rate = dec.rate();
        if fmt != ffmpeg::format::Sample::None {
            resampler = ffmpeg::software::resampling::Context::get(
                fmt,
                layout,
                rate,
                ffmpeg::format::Sample::F32(ffmpeg::format::sample::Type::Packed),
                ffmpeg::ChannelLayout::STEREO,
                SAMPLE_RATE,
            )
            .ok();
        }
    }

    // Playback clock. `pos` is the virtual position at `base` instant.
    let mut base = Instant::now();
    let mut pos = seek.max(0.0);
    let mut playing = true;
    let mut last_frame = None;

    if std::env::var("FEDITEXTER_MEDIA_TRACE").is_ok() {
        eprintln!("[media] entering decode loop");
    }

    let mut demux = input.packets();
    let mut video_tb = None;
    let mut iter: u32 = 0;

    'outer: loop {
        iter += 1;
        // Command intake between frames.
        loop {
            match cmd_rx.try_recv() {
                Ok(Cmd::Stop) => return Ok(SessionExit::Stop),
                Ok(cmd) => {
                    if std::env::var("FEDITEXTER_VIEWER_TRACE").is_ok() {
                        let name = match &cmd {
                            Cmd::Play => "Play",
                            Cmd::Pause => "Pause",
                            Cmd::Seek(_) => "Seek",
                            Cmd::Volume(_) => "Volume",
                            Cmd::Stop => "Stop",
                        };
                        eprintln!("[media] intake {name} (iter {iter})");
                    }
                    match handle_cmd(cmd, &mut playing, &mut pos, &mut base, volume) {
                        CmdAction::Continue => {}
                        CmdAction::Exit(exit) => return Ok(exit),
                    }
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => return Ok(SessionExit::Stop),
            }
        }
        if iter % 300 == 0 && std::env::var("FEDITEXTER_VIEWER_TRACE").is_ok() {
            eprintln!("[media] iter {iter} heartbeat playing={playing}");
        }

        state.store(if playing { 1 } else { 2 }, Ordering::Relaxed);

        if !playing {
            position.store((pos as f32).to_bits(), Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(20));
            continue;
        }

        // Audio catch-up: keep the ring topped up but bounded.
        let mut audio_pushed = false;

        let Some((stream, packet)) = demux.next() else {
            if std::env::var("FEDITEXTER_MEDIA_TRACE").is_ok() {
                eprintln!("[media] demux ended immediately");
            }
            let _ = ev_tx.send(MediaEvent::Ended);
            state.store(2, Ordering::Relaxed);
            // Wait for replay / seek / stop.
            loop {
                match cmd_rx.recv() {
                    Ok(Cmd::Stop) => return Ok(SessionExit::Stop),
                    Ok(Cmd::Seek(t)) => return Ok(SessionExit::Seek(t.max(0.0))),
                    Ok(Cmd::Play) => {
                        state.store(1, Ordering::Relaxed);
                        return Ok(SessionExit::Seek(0.0));
                    }
                    _ => {}
                }
            }
        };

        let idx = stream.index();

        if Some(idx) == video_index {
            if let Some(dec) = video_decoder.as_mut() {
                if video_tb.is_none() {
                    video_tb = Some(f64::from(stream.time_base()));
                }
                if dec.send_packet(&packet).is_ok() {
                    let mut frame = ffmpeg::frame::Video::empty();
                    loop {
                        match dec.receive_frame(&mut frame) {
                            Ok(()) => {
                                let pts = frame.pts().unwrap_or(0) as f64;
                                let tb = video_tb.unwrap_or(0.0);
                                let frame_pos = pts * tb;
                                // GOP catch-up frames decoded between the seek
                                // keyframe and the target — drop them silently.
                                if frame_pos < seek {
                                    continue;
                                }
                                if let Some(sc) = scaler.as_mut() {
                                    let mut out = ffmpeg::frame::Video::empty();
                                    if sc.run(&frame, &mut out).is_ok() {
                                        let w = out.width();
                                        let h = out.height();
                                        let rgba = pack_rgba(&out);
                                        last_frame = Some(MediaEvent::Frame {
                                            width: w,
                                            height: h,
                                            rgba: Arc::new(rgba),
                                        });
                                    }
                                }
                                // Pace to the frame's presentation time.
                                let wall_pos = pos + base.elapsed().as_secs_f64();
                                let delay = frame_pos - wall_pos;
                                if delay > 0.0 {
                                    let mut remain = delay;
                                    while remain > 0.0 {
                                        // Wake up for seek/pause without long sleeps.
                                        // NOTE: recv_timeout CONSUMES the command, so
                                        // process it here — `continue 'outer` alone
                                        // would drop it (the intake's try_recv then
                                        // finds an empty channel).
                                        match cmd_rx.recv_timeout(Duration::from_secs_f64(remain.min(0.05))) {
                                            Ok(cmd) => {
                                                match handle_cmd(cmd, &mut playing, &mut pos, &mut base, volume) {
                                                    CmdAction::Continue => continue 'outer,
                                                    CmdAction::Exit(exit) => return Ok(exit),
                                                }
                                            }
                                            Err(_) => {}
                                        }
                                        remain -= 0.05;
                                    }
                                }
                                if let Some(ev) = last_frame.take() {
                                    let _ = ev_tx.send(ev);
                                }
                                let p = frame_pos.max(wall_pos);
                                if p.is_finite() {
                                    // Re-anchor the playback clock to the
                                    // present so `base.elapsed()` only accrues
                                    // since the last presented frame. Otherwise
                                    // `wall_pos` keeps counting absolute session
                                    // time, delay goes negative and playback
                                    // races at decode speed.
                                    pos = p;
                                    base = Instant::now();
                                }
                                position.store((pos as f32).to_bits(), Ordering::Relaxed);
                            }
                            Err(ffmpeg::Error::Other { .. })
                            | Err(ffmpeg::Error::Eof)
                            | Err(ffmpeg::Error::InvalidData) => break,
                            Err(_) => break,
                        }
                    }
                }
            }
        } else if Some(idx) == audio_index {
            if let Some(dec) = audio_decoder.as_mut() {
                if dec.send_packet(&packet).is_ok() {
                    let mut frame = ffmpeg::frame::Audio::empty();
                    loop {
                        match dec.receive_frame(&mut frame) {
                            Ok(()) => {
                                let mut out = ffmpeg::frame::Audio::empty();
                                if let Some(rs) = resampler.as_mut() {
                                    if rs.run(&frame, &mut out).is_ok() {
                                        push_audio(&ring, &out);
                                    }
                                }
                                audio_pushed = true;
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
            let _ = audio_pushed;
        }
    }
}

fn pack_rgba(frame: &ffmpeg::frame::Video) -> Vec<u8> {
    let width = frame.width() as usize;
    let height = frame.height() as usize;
    let data = frame.data(0);
    let stride = frame.stride(0);
    if data.len() < stride * height {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(width * height * 4);
    for row in data.chunks_exact(stride).take(height) {
        out.extend_from_slice(&row[..width * 4]);
    }
    out
}

fn push_audio(ring: &Arc<Mutex<AudioRing>>, frame: &ffmpeg::frame::Audio) {
    let samples = frame.plane::<f32>(0);
    let mut guard = ring.lock().unwrap();
    for s in samples {
        guard.data.push_back(*s);
    }
    let over = guard.data.len().saturating_sub(AUDIO_RING_MAX);
    if over > 0 {
        guard.data.drain(..over);
    }
}

fn open_output(
    ring: Arc<Mutex<AudioRing>>,
    volume: Arc<AtomicU32>,
) -> Option<cpal::Stream> {
    let host = cpal::default_host();
    let device = host.default_output_device()?;
    let (config, format) = pick_output_config(&device)?;
    let channels = config.channels.max(1) as usize;
    let data = move |out: &mut [f32], _: &cpal::OutputCallbackInfo| {
        let vol = f32::from_bits(volume.load(Ordering::Relaxed));
        let mut guard = ring.lock().unwrap();
        for chunk in out.chunks_mut(channels) {
            let s = match guard.data.pop_front() {
                Some(s) => s * vol,
                None => 0.0,
            };
            for c in chunk.iter_mut() {
                *c = s.clamp(-1.0, 1.0);
            }
        }
    };
    let err = |e: cpal::Error| {
        eprintln!("[media] output stream error: {e}");
    };
    match format {
        cpal::SampleFormat::F32 => {
            device
                .build_output_stream::<f32, _, _>(config, data, err, None)
                .ok()
        }
        _ => None,
    }
    .inspect(|s| {
        let _ = s.play();
    })
}

/// Holds a cpal output stream opened on a helper thread. Since `open_output`
/// can block on a busy audio device, the decode thread never calls it directly:
/// audio starts in the background and silence is emitted until samples arrive.
struct AudioHandle {
    stop: Arc<AtomicBool>,
}

impl Drop for AudioHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

fn spawn_output(
    ring: Arc<Mutex<AudioRing>>,
    volume: Arc<AtomicU32>,
) -> AudioHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_t = stop.clone();
    let _ = std::thread::Builder::new()
        .name("media-audio".into())
        .spawn(move || {
            let _stream = open_output(ring, volume);
            while !stop_t.load(Ordering::Relaxed) {
                std::thread::park_timeout(Duration::from_millis(500));
            }
        });
    AudioHandle { stop }
}

fn pick_output_config(dev: &cpal::Device) -> Option<(cpal::StreamConfig, cpal::SampleFormat)> {
    let supported = dev.supported_output_configs().ok()?;
    for c in supported {
        if c.sample_format() == cpal::SampleFormat::F32
            && c.min_sample_rate() <= SAMPLE_RATE
            && c.max_sample_rate() >= SAMPLE_RATE
        {
            let cfg = c.with_sample_rate(SAMPLE_RATE).config();
            return Some((cfg, cpal::SampleFormat::F32));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pacing_measure() {
        let path = std::env::var("FT_TEST_VIDEO")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| "/var/folders/_8/57kvqnk52kd94w0525ppqqz80000gn/T/opencode/videotest/sample.mp4".into());
        if !path.exists() {
            eprintln!("[test] sample missing");
            return;
        }
        let engine = MediaEngine::open(path.clone(), 0.0, 1.0);
        let rx = engine.events();
        let mut g = rx.lock().unwrap();
        let t0 = std::time::Instant::now();
        let mut frames = Vec::new();
        let mut last_sent = None;
        while std::time::Instant::now() - t0 < std::time::Duration::from_secs(6) {
            match g.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(MediaEvent::Frame { .. }) => {
                    let now = last_sent.unwrap_or(t0);
                    let dt = now.elapsed().as_secs_f64();
                    frames.push(dt);
                    last_sent = Some(std::time::Instant::now());
                }
                Ok(MediaEvent::Opened { .. }) => {}
                Ok(MediaEvent::Ended) => { eprintln!("[test] ended"); break; }
                Ok(MediaEvent::Error(e)) => { eprintln!("[test] err {e}"); break; }
                Err(_) => {}
            }
        }
        eprintln!("[test] frames={} first10_dt={:?}", frames.len(), &frames[..frames.len().min(10)]);
    }

    #[test]
    fn seek_jumps() {
        let path = std::env::var("FT_TEST_VIDEO")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| "/var/folders/_8/57kvqnk52kd94w0525ppqqz80000gn/T/opencode/videotest/sample.mp4".into());
        if !path.exists() {
            eprintln!("[test] sample missing");
            return;
        }
        let engine = MediaEngine::open(path.clone(), 0.0, 1.0);
        let rx = engine.events();
        let mut g = rx.lock().unwrap();
        let t0 = std::time::Instant::now();
        let mut duration = 0.0f64;
        while std::time::Instant::now() - t0 < std::time::Duration::from_secs(3) {
            match g.recv_timeout(std::time::Duration::from_millis(200)) {
                Ok(MediaEvent::Opened { duration: d, .. }) => { duration = d; break; }
                Ok(_) => {}
                Err(_) => {}
            }
        }
        assert!(duration > 0.0, "no Opened event");

        let target = duration * 0.7;
        engine.seek(target);
        let t1 = std::time::Instant::now();
        let mut pos = None;
        while std::time::Instant::now() - t1 < std::time::Duration::from_secs(3) {
            match g.recv_timeout(std::time::Duration::from_millis(200)) {
                Ok(MediaEvent::Frame { .. }) => {
                    let p = engine.position_secs();
                    if p > (target - 1.0) as f32 {
                        pos = Some(p);
                        break;
                    }
                }
                Ok(_) => {}
                Err(_) => {}
            }
        }
        eprintln!("[test] seek to {target} position_secs={:?}", pos);
        assert!(pos.unwrap_or(0.0) >= (target - 1.0) as f32, "seek did not jump: {pos:?}");
    }

    #[test]
    fn seek_during_active_playback() {
        let path = std::env::var("FT_TEST_VIDEO")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| "/var/folders/_8/57kvqnk52kd94w0525ppqqz80000gn/T/opencode/videotest/sample.mp4".into());
        if !path.exists() {
            eprintln!("[test] sample missing");
            return;
        }
        let engine = MediaEngine::open(path.clone(), 0.0, 1.0);
        let rx = engine.events();
        let mut g = rx.lock().unwrap();
        let t0 = std::time::Instant::now();
        let mut frames = 0usize;
        while std::time::Instant::now() - t0 < std::time::Duration::from_millis(1500) {
            match g.recv_timeout(std::time::Duration::from_millis(200)) {
                Ok(MediaEvent::Frame { .. }) => frames += 1,
                Ok(_) => {}
                Err(_) => {}
            }
        }
        eprintln!("[test] drained {frames} frames in 1.5s");
        assert!(frames > 5, "video not playing");

        engine.seek(10.0);
        let t1 = std::time::Instant::now();
        let mut jumped = None;
        while std::time::Instant::now() - t1 < std::time::Duration::from_secs(3) {
            match g.recv_timeout(std::time::Duration::from_millis(200)) {
                Ok(MediaEvent::Frame { .. }) => {
                    let p = engine.position_secs();
                    if p > 9.0 {
                        jumped = Some(p);
                        break;
                    }
                }
                Ok(_) => {}
                Err(_) => {}
            }
        }
        eprintln!("[test] seek to 10 during playback -> jumped={jumped:?}");
        assert!(jumped.is_some(), "seek did not work during active playback");
    }

    #[test]
    fn seek_through_taken_engine() {
        let path = std::env::var("FT_TEST_VIDEO")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| "/var/folders/_8/57kvqnk52kd94w0525ppqqz80000gn/T/opencode/videotest/sample.mp4".into());
        if !path.exists() {
            eprintln!("[test] sample missing");
            return;
        }
        let engine = MediaEngine::open(path.clone(), 0.0, 1.0);
        let mut holder: Option<MediaEngine> = Some(engine);
        let rx = holder.as_ref().unwrap().events();
        let mut g = rx.lock().unwrap();
        let t0 = std::time::Instant::now();
        while std::time::Instant::now() - t0 < std::time::Duration::from_millis(1000) {
            let _ = g.recv_timeout(std::time::Duration::from_millis(100));
        }
        let mut e = holder.take().unwrap();
        e.seek(10.0);
        holder = Some(e);
        let e = holder.as_ref().unwrap();
        let t1 = std::time::Instant::now();
        let mut jumped = None;
        while std::time::Instant::now() - t1 < std::time::Duration::from_secs(3) {
            if let Ok(MediaEvent::Frame { .. }) = g.recv_timeout(std::time::Duration::from_millis(200)) {
                let p = e.position_secs();
                if p > 9.0 {
                    jumped = Some(p);
                    break;
                }
            }
        }
        eprintln!("[test] seek through taken engine -> jumped={jumped:?}");
        assert!(jumped.is_some(), "seek broken after take/put");
    }

    #[test]
    fn pause_during_active_playback() {
        let path = std::env::var("FT_TEST_VIDEO")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| "/var/folders/_8/57kvqnk52kd94w0525ppqqz80000gn/T/opencode/videotest/sample.mp4".into());
        if !path.exists() {
            eprintln!("[test] sample missing");
            return;
        }
        let engine = MediaEngine::open(path.clone(), 0.0, 1.0);
        let rx = engine.events();
        let mut g = rx.lock().unwrap();
        let t0 = std::time::Instant::now();
        while std::time::Instant::now() - t0 < std::time::Duration::from_millis(1000) {
            match g.recv_timeout(std::time::Duration::from_millis(200)) {
                Ok(MediaEvent::Frame { .. }) => {}
                Ok(_) => {}
                Err(_) => {}
            }
        }
        engine.pause();
        let t1 = std::time::Instant::now();
        let mut frames_after = 0usize;
        while std::time::Instant::now() - t1 < std::time::Duration::from_millis(800) {
            match g.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(MediaEvent::Frame { .. }) => frames_after += 1,
                Ok(_) => {}
                Err(_) => {}
            }
        }
        eprintln!("[test] frames after pause: {frames_after} (expect ~0)");
        assert!(frames_after <= 2, "video did not pause: {frames_after} frames after pause");
    }

    #[test]
    fn frames_decode_at_source_resolution() {
        let path = std::env::var("FT_TEST_VIDEO")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| "/var/folders/_8/57kvqnk52kd94w0525ppqqz80000gn/T/opencode/videotest/sample.mp4".into());
        if !path.exists() {
            eprintln!("[test] sample missing");
            return;
        }
        let engine = MediaEngine::open(path.clone(), 0.0, 1.0);
        let rx = engine.events();
        let mut g = rx.lock().unwrap();
        let t0 = std::time::Instant::now();
        let mut biggest = 0usize;
        let mut checked = 0usize;
        while std::time::Instant::now() - t0 < std::time::Duration::from_secs(3) {
            match g.recv_timeout(std::time::Duration::from_millis(200)) {
                Ok(MediaEvent::Frame { rgba, .. }) => {
                    biggest = biggest.max(rgba.len());
                    checked += 1;
                    if checked >= 5 {
                        break;
                    }
                }
                Ok(_) => {}
                Err(_) => {}
            }
        }
        eprintln!("[test] biggest frame rgba={biggest} bytes ({checked} frames)");
        assert!(checked > 0, "no frames decoded");
        assert!(biggest > 0, "empty frame");
    }

    #[test]
    fn decodes_video_frames_to_rgba() {
        let dir = std::env::temp_dir();
        let path = dir.join("ft-media-test.mp4");
        let src = "/Users/andromeda/PathApps/ffmpeg";
        let out = std::process::Command::new(src)
            .args([
                "-y", "-f", "lavfi", "-i", "testsrc=duration=1:size=64x48:rate=10",
                "-pix_fmt", "yuv420p",
                "-c:v", "libx264",
                "-an",
            ])
            .arg(&path)
            .output();
        if !out.as_ref().map(|o| o.status.success()).unwrap_or(false) {
            return;
        }
        let engine = MediaEngine::open(path.clone(), 0.0, 1.0);
        let rx = engine.events();
        let mut got_frame = false;
        let mut got_sized = false;
        let mut rx_guard = rx.lock().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
        while std::time::Instant::now() < deadline {
            match rx_guard.recv_timeout(std::time::Duration::from_millis(200)) {
                Ok(MediaEvent::Frame { width, height, rgba }) => {
                    got_frame = true;
                    if width == 64 && height == 48 && rgba.len() == 64 * 48 * 4 {
                        got_sized = true;
                    }
                    break;
                }
                Ok(MediaEvent::Ended) => break,
                Ok(MediaEvent::Error(e)) => {
                    eprintln!("[test] engine error: {e}");
                    break;
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
        engine.play();
        let _ = engine.pause();
        let _ = engine.seek(1.0);
        std::mem::drop(engine);
        let _ = std::fs::remove_file(&path);
        assert!(got_frame, "expected at least one decoded frame");
        assert!(got_sized, "expected 64x48x4 RGBA frame");
        let _ = got_frame;
    }
}
