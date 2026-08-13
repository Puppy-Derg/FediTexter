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

    'outer: loop {
        // Command intake between frames.
        loop {
            match cmd_rx.try_recv() {
                Ok(Cmd::Stop) => return Ok(SessionExit::Stop),
                Ok(Cmd::Seek(t)) => return Ok(SessionExit::Seek(t.max(0.0))),
                Ok(Cmd::Play) => {
                    if !playing {
                        playing = true;
                        base = Instant::now();
                    }
                }
                Ok(Cmd::Pause) => {
                    if playing {
                        pos += base.elapsed().as_secs_f64();
                        playing = false;
                    }
                }
                Ok(Cmd::Volume(v)) => {
                    volume.store(v.to_bits(), Ordering::Relaxed);
                    let _ = v;
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => return Ok(SessionExit::Stop),
            }
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
                                        if cmd_rx.recv_timeout(Duration::from_secs_f64(remain.min(0.05))).is_ok() {
                                            continue 'outer;
                                        }
                                        remain -= 0.05;
                                    }
                                }
                                if let Some(ev) = last_frame.take() {
                                    let _ = ev_tx.send(ev);
                                }
                                let p = frame_pos.max(wall_pos);
                                if p.is_finite() {
                                    pos = p;
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
