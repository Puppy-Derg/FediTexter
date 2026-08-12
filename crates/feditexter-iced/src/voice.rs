//! Voice channels: real-time audio (and optional camera/screen video) over a
//! WebRTC P2P mesh, relayed through the server's WS signaling hub.
//!
//! When you join a voice channel the server replies with the current occupant
//! list (`voice_state`); the joiner then builds one peer connection per occupant
//! and sends an SDP `voice_offer`. Existing members answer. Every connection
//! carries three tracks: microphone (Opus), camera (H.264) and screen (H.264).
//! Toggling the camera or screen only starts/stops writing frames to those
//! tracks — no renegotiation is ever needed. Remote Opus is decoded and mixed
//! into the output device; remote H.264 is decoded and surfaced to the UI as
//! RGBA frames for live tiles.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cpal::traits::{DeviceTrait, HostTrait};
use cpal::Sample;
use serde_json::json;
use tokio::sync::mpsc::UnboundedSender;

use webrtc::media_stream::track_local::static_sample::TrackLocalStaticSample;
use webrtc::media_stream::track_remote::{TrackRemote, TrackRemoteEvent};
use webrtc::media_stream::Track;
use webrtc::peer_connection::{
    MediaEngine, PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler,
    RTCPeerConnectionIceEvent, RTCPeerConnectionState, RTCConfigurationBuilder, RTCIceServer,
    RTCSessionDescription, Registry, register_default_interceptors,
};
use webrtc::runtime::default_runtime;

use rtc::media_stream::MediaStreamTrack;
use rtc::rtp::packetizer::Depacketizer;
use rtc::rtp_transceiver::rtp_sender::{
    RTCRtpCodec, RTCRtpCodingParameters, RTCRtpEncodingParameters, RtpCodecKind,
};
use rtc::shared::time::SystemInstant;

use crate::p2p::SignalEvent;

/// Negotiated payload types: both peers run identical builds registering the
/// default codecs, so Opus is 111 and H.264 (packetization-mode=1) is 102.
const PT_OPUS: u8 = 111;
const PT_H264: u8 = 102;
const SAMPLE_RATE: u32 = 48_000;
const FRAME_SAMPLES: usize = 960; // 20 ms at 48 kHz
const CAMERA_MAX_WIDTH: u32 = 640;
const SCREEN_MAX_WIDTH: u32 = 1280;

/// Which video source a remote track carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VoiceVideoKind {
    Camera,
    Screen,
}

/// Events surfaced to the UI.
#[derive(Clone, Debug)]
pub enum VoiceEvent {
    Joined { guild_id: u64, channel_id: u64 },
    MemberJoined { user_id: u64, username: String },
    MemberLeft { user_id: u64 },
    /// One decoded video frame from a remote peer.
    Video {
        user_id: u64,
        kind: VoiceVideoKind,
        width: u32,
        height: u32,
        rgba: Vec<u8>,
    },
    Left,
    Error(String),
}

/// Per-peer output buffers (mono f32 at 48 kHz), drained by the output device.
pub struct OutputMix {
    pub buffers: HashMap<u64, VecDeque<f32>>,
}

impl Default for OutputMix {
    fn default() -> Self {
        Self { buffers: HashMap::new() }
    }
}

struct VoiceInner {
    channel_id: Option<u64>,
    guild_id: Option<u64>,
    self_user_id: u64,
    members: HashMap<u64, String>,
    peers: HashMap<u64, Arc<dyn PeerConnection>>,
    pending_ice: HashMap<u64, Vec<webrtc::peer_connection::RTCIceCandidateInit>>,
    /// peer -> (ssrc, mic track)
    audio_tracks: HashMap<u64, (u32, TrackLocalStaticSample)>,
    /// peer -> kind -> (ssrc, video track)
    video_tracks: HashMap<u64, HashMap<VoiceVideoKind, (u32, TrackLocalStaticSample)>>,
    muted: bool,
    camera_on: bool,
    screen_on: bool,
    camera_stop: Option<Arc<AtomicBool>>,
    screen_stop: Option<Arc<AtomicBool>>,
    audio_running: bool,
    audio_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Default for VoiceInner {
    fn default() -> Self {
        Self {
            channel_id: None,
            guild_id: None,
            self_user_id: 0,
            members: HashMap::new(),
            peers: HashMap::new(),
            pending_ice: HashMap::new(),
            audio_tracks: HashMap::new(),
            video_tracks: HashMap::new(),
            muted: false,
            camera_on: false,
            screen_on: false,
            camera_stop: None,
            screen_stop: None,
            audio_running: false,
            audio_shutdown: None,
        }
    }
}

pub struct VoiceManager {
    handle: Arc<tokio::runtime::Handle>,
    ws_tx: UnboundedSender<String>,
    ui_tx: UnboundedSender<VoiceEvent>,
    inner: Mutex<VoiceInner>,
    mix: Arc<Mutex<OutputMix>>,
}

impl std::fmt::Debug for VoiceManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VoiceManager").finish_non_exhaustive()
    }
}

impl VoiceManager {
    pub fn new(
        handle: tokio::runtime::Handle,
        ws_tx: UnboundedSender<String>,
        ui_tx: UnboundedSender<VoiceEvent>,
    ) -> Arc<Self> {
        Arc::new(Self {
            handle: Arc::new(handle),
            ws_tx,
            ui_tx,
            inner: Mutex::new(VoiceInner::default()),
            mix: Arc::new(Mutex::new(OutputMix::default())),
        })
    }

    pub fn in_channel(&self) -> Option<u64> {
        self.inner.lock().unwrap().channel_id
    }

    pub fn members(&self) -> Vec<(u64, String)> {
        let inner = self.inner.lock().unwrap();
        let mut out: Vec<(u64, String)> =
            inner.members.iter().map(|(u, n)| (*u, n.clone())).collect();
        out.sort_by_key(|(u, _)| *u);
        out
    }

    pub fn is_muted(&self) -> bool {
        self.inner.lock().unwrap().muted
    }

    pub fn camera_on(&self) -> bool {
        self.inner.lock().unwrap().camera_on
    }

    pub fn screen_on(&self) -> bool {
        self.inner.lock().unwrap().screen_on
    }

    /// Join a voice channel. Presence is announced over the WS; the server
    /// replies with the occupant list via `handle_voice_state`.
    pub fn join(self: &Arc<Self>, guild_id: u64, channel_id: u64, self_user_id: u64) {
        if self.in_channel().is_some() {
            self.leave();
        }
        {
            let mut inner = self.inner.lock().unwrap();
            inner.channel_id = Some(channel_id);
            inner.guild_id = Some(guild_id);
            inner.self_user_id = self_user_id;
        }
        let _ = self.ws_tx.send(
            json!({ "type": "voice_join", "guild_id": guild_id, "channel_id": channel_id }).to_string(),
        );
    }

    pub fn leave(self: &Arc<Self>) {
        let (guild_id, channel_id) = {
            let mut inner = self.inner.lock().unwrap();
            (inner.guild_id.take(), inner.channel_id.take())
        };
        if let (Some(g), Some(c)) = (guild_id, channel_id) {
            let _ = self.ws_tx.send(
                json!({ "type": "voice_leave", "guild_id": g, "channel_id": c }).to_string(),
            );
        }
        let peers: Vec<u64> = {
            let mut inner = self.inner.lock().unwrap();
            inner.members.clear();
            inner.peers.keys().cloned().collect()
        };
        for peer in peers {
            self.hangup_peer(peer);
        }
        if let Some(tx) = self.inner.lock().unwrap().audio_shutdown.take() {
            let _ = tx.send(());
        }
        {
            let mut inner = self.inner.lock().unwrap();
            inner.audio_running = false;
            inner.camera_on = false;
            inner.screen_on = false;
            if let Some(flag) = inner.camera_stop.take() {
                flag.store(false, Ordering::Relaxed);
            }
            if let Some(flag) = inner.screen_stop.take() {
                flag.store(false, Ordering::Relaxed);
            }
        }
        let _ = self.ui_tx.send(VoiceEvent::Left);
    }

    pub fn set_muted(&self, muted: bool) {
        self.inner.lock().unwrap().muted = muted;
    }

    pub fn set_camera(self: &Arc<Self>, on: bool) {
        {
            let mut inner = self.inner.lock().unwrap();
            if inner.camera_on == on {
                return;
            }
            inner.camera_on = on;
        }
        if on {
            spawn_camera(self.clone());
        } else if let Some(flag) = self.inner.lock().unwrap().camera_stop.take() {
            flag.store(false, Ordering::Relaxed);
        }
    }

    pub fn set_screen(self: &Arc<Self>, on: bool) {
        {
            let mut inner = self.inner.lock().unwrap();
            if inner.screen_on == on {
                return;
            }
            inner.screen_on = on;
        }
        if on {
            spawn_screen(self.clone());
        } else if let Some(flag) = self.inner.lock().unwrap().screen_stop.take() {
            flag.store(false, Ordering::Relaxed);
        }
    }

    fn send_signal(&self, kind: &str, to_user_id: u64, data: &str) {
        let channel_id = match self.inner.lock().unwrap().channel_id {
            Some(c) => c,
            None => return,
        };
        let _ = self.ws_tx.send(
            json!({
                "type": kind,
                "to_user_id": to_user_id,
                "file_id": format!("voice-{channel_id}"),
                "data": data,
            })
            .to_string(),
        );
    }

    /// Server pushed the occupant list right after we joined.
    pub fn handle_voice_state(self: &Arc<Self>, channel_id: u64, users: Vec<(u64, String)>) {
        let (guild_id, is_current) = {
            let inner = self.inner.lock().unwrap();
            (inner.guild_id, inner.channel_id == Some(channel_id))
        };
        if !is_current {
            return;
        }
        {
            let mut inner = self.inner.lock().unwrap();
            inner.members.clear();
            for (uid, name) in users.iter() {
                inner.members.insert(*uid, name.clone());
            }
        }
        if !self.inner.lock().unwrap().audio_running {
            self.spawn_audio();
        }
        for (uid, _) in users {
            self.ensure_peer_offer(uid);
        }
        if let Some(g) = guild_id {
            let _ = self.ui_tx.send(VoiceEvent::Joined { guild_id: g, channel_id });
        }
    }

    /// Another member joined or left the channel we're in.
    pub fn handle_voice_presence(
        self: &Arc<Self>,
        channel_id: u64,
        user_id: u64,
        username: String,
        joined: bool,
    ) {
        let current = self.inner.lock().unwrap().channel_id == Some(channel_id);
        if !current {
            return;
        }
        let self_id = self.inner.lock().unwrap().self_user_id;
        if joined && user_id != self_id {
            {
                let mut inner = self.inner.lock().unwrap();
                inner.members.insert(user_id, username.clone());
            }
            // The joiner always initiates: they got our id in their `voice_state`
            // and will send us a `voice_offer`; we answer when it arrives. Offering
            // here too would build a second, conflicting connection per pair.
            let _ = self.ui_tx.send(VoiceEvent::MemberJoined { user_id, username });
        } else if !joined {
            {
                let mut inner = self.inner.lock().unwrap();
                inner.members.remove(&user_id);
            }
            self.hangup_peer(user_id);
            let _ = self.ui_tx.send(VoiceEvent::MemberLeft { user_id });
        }
    }

    /// Dispatch an inbound `voice_*` signaling message.
    pub fn handle_signal(self: &Arc<Self>, sig: SignalEvent) {
        let peer_id = sig.from_user_id.unwrap_or(0);
        if peer_id == 0 || !sig.file_id.starts_with("voice-") {
            return;
        }
        match sig.kind.as_str() {
            "voice_offer" => self.on_offer(sig, peer_id),
            "voice_answer" => self.on_answer(sig, peer_id),
            "voice_ice" => self.on_ice(sig, peer_id),
            "voice_hangup" => self.hangup_peer(peer_id),
            _ => {}
        }
    }

    fn ensure_peer_offer(self: &Arc<Self>, peer_id: u64) {
        if self.inner.lock().unwrap().peers.contains_key(&peer_id) {
            return;
        }
        let mgr = self.clone();
        self.handle.spawn(async move {
            if let Err(e) = mgr.establish_peer(peer_id).await {
                eprintln!("[voice] offer to user {peer_id} failed: {e}");
            }
        });
    }

    /// Build a peer connection toward `peer_id`, add the three tracks, register
    /// it and send an SDP offer. This side is always the offerer.
    async fn establish_peer(
        self: &Arc<Self>,
        peer_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pc = build_voice_peer(self.clone(), peer_id).await?;
        add_local_tracks(self, peer_id, pc.clone()).await?;
        self.register_peer(peer_id, pc.clone()).await?;
        let offer = pc.create_offer(None).await?;
        pc.set_local_description(offer.clone()).await?;
        let sdp = serde_json::to_string(&offer)?;
        self.send_signal("voice_offer", peer_id, &sdp);
        Ok(())
    }

    fn on_offer(self: &Arc<Self>, sig: SignalEvent, peer_id: u64) {
        let Ok(sdp) = serde_json::from_str::<RTCSessionDescription>(
            sig.data.as_deref().unwrap_or_default(),
        ) else {
            return;
        };
        let mgr = self.clone();
        self.handle.spawn(async move {
            let result = async {
                let existing = mgr.inner.lock().unwrap().peers.get(&peer_id).cloned();
                let pc = if let Some(pc) = existing {
                    pc // renegotiation (e.g. after a re-offer)
                } else {
                    let pc = build_voice_peer(mgr.clone(), peer_id).await?;
                    add_local_tracks(&mgr, peer_id, pc.clone()).await?;
                    mgr.register_peer(peer_id, pc.clone()).await?;
                    pc
                };
                pc.set_remote_description(sdp).await?;
                let answer = pc.create_answer(None).await?;
                pc.set_local_description(answer.clone()).await?;
                let sdp = serde_json::to_string(&answer)?;
                mgr.send_signal("voice_answer", peer_id, &sdp);
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }
            .await;
            if let Err(e) = result {
                eprintln!("[voice] answer to user {peer_id} failed: {e}");
            }
        });
    }

    fn on_answer(&self, sig: SignalEvent, peer_id: u64) {
        let Ok(sdp) = serde_json::from_str::<RTCSessionDescription>(
            sig.data.as_deref().unwrap_or_default(),
        ) else {
            return;
        };
        let pc = self.inner.lock().unwrap().peers.get(&peer_id).cloned();
        if let Some(pc) = pc {
            let handle = self.handle.clone();
            handle.spawn(async move {
                let _ = pc.set_remote_description(sdp).await;
            });
        }
    }

    fn on_ice(&self, sig: SignalEvent, peer_id: u64) {
        let Ok(init) = serde_json::from_str::<webrtc::peer_connection::RTCIceCandidateInit>(
            sig.data.as_deref().unwrap_or_default(),
        ) else {
            return;
        };
        let pc = {
            let mut inner = self.inner.lock().unwrap();
            match inner.peers.get(&peer_id).cloned() {
                Some(pc) => Some(pc),
                None => {
                    inner.pending_ice.entry(peer_id).or_default().push(init.clone());
                    None
                }
            }
        };
        if let Some(pc) = pc {
            let handle = self.handle.clone();
            handle.spawn(async move {
                let _ = pc.add_ice_candidate(init).await;
            });
        }
    }

    async fn register_peer(
        &self,
        peer_id: u64,
        pc: Arc<dyn PeerConnection>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pending = {
            let mut inner = self.inner.lock().unwrap();
            inner.peers.insert(peer_id, pc.clone());
            inner.pending_ice.remove(&peer_id).unwrap_or_default()
        };
        for init in pending {
            let _ = pc.add_ice_candidate(init).await;
        }
        Ok(())
    }

    fn hangup_peer(&self, peer_id: u64) {
        let pc = {
            let mut inner = self.inner.lock().unwrap();
            inner.audio_tracks.remove(&peer_id);
            inner.video_tracks.remove(&peer_id);
            inner.pending_ice.remove(&peer_id);
            inner.peers.remove(&peer_id)
        };
        if let Some(pc) = pc {
            let handle = self.handle.clone();
            handle.spawn(async move {
                let _ = pc.close().await;
            });
        }
        self.mix.lock().unwrap().buffers.remove(&peer_id);
    }

    // ------------------------------------------------------------------ audio

    fn spawn_audio(self: &Arc<Self>) {
        {
            let mut inner = self.inner.lock().unwrap();
            if inner.audio_running {
                return;
            }
            inner.audio_running = true;
        }
        let host = cpal::default_host();
        let Some(in_dev) = host.default_input_device() else {
            self.ui_err("no microphone found".into());
            self.inner.lock().unwrap().audio_running = false;
            return;
        };
        let Some(out_dev) = host.default_output_device() else {
            self.ui_err("no audio output device found".into());
            self.inner.lock().unwrap().audio_running = false;
            return;
        };
        let (in_config, in_format) = pick_input_config(&in_dev).unwrap_or_else(|| {
            in_dev
                .default_input_config()
                .map(|c| (c.config(), c.sample_format()))
                .unwrap_or((
                    cpal::StreamConfig {
                        channels: 1,
                        sample_rate: 48_000,
                        buffer_size: cpal::BufferSize::Default,
                    },
                    cpal::SampleFormat::F32,
                ))
        });
        let (out_config, out_format) = pick_output_config(&out_dev).unwrap_or_else(|| {
            out_dev
                .default_output_config()
                .map(|c| (c.config(), c.sample_format()))
                .unwrap_or((
                    cpal::StreamConfig {
                        channels: 2,
                        sample_rate: 48_000,
                        buffer_size: cpal::BufferSize::Default,
                    },
                    cpal::SampleFormat::F32,
                ))
        });

        let (mic_tx, mut mic_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<f32>>();
        let err = |e| eprintln!("[voice] input stream error: {e}");
        let in_stream = build_input_stream(&in_dev, in_config, in_format, mic_tx.clone(), err);
        let in_stream = match in_stream {
            Ok(s) => s,
            Err(e) => {
                self.ui_err(format!("could not open microphone: {e}"));
                self.inner.lock().unwrap().audio_running = false;
                return;
            }
        };
        let err = |e| eprintln!("[voice] output stream error: {e}");
        let mix = self.mix.clone();
        let out_channels = out_config.channels.max(1) as usize;
        let out_rate = out_config.sample_rate as f32;
        let mut octx = OutputCtx {
            mix,
            rate_out: out_rate,
            resampler: LinearResampler::new(out_rate / SAMPLE_RATE as f32),
            scratch: Vec::new(),
        };
        let out_stream = build_output_stream(
            &out_dev,
            out_config,
            out_format,
            move |data: &mut [f32]| {
                fill_output(data, &mut octx, out_channels);
            },
            err,
        );
        let out_stream = match out_stream {
            Ok(s) => s,
            Err(e) => {
                self.ui_err(format!("could not open audio output: {e}"));
                self.inner.lock().unwrap().audio_running = false;
                return;
            }
        };

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        self.inner.lock().unwrap().audio_shutdown = Some(shutdown_tx);

        let mgr = self.clone();
        let in_rate = in_config.sample_rate as f32;
        let handle = self.handle.clone();
        handle.spawn(async move {
            let _keep_streams = (in_stream, out_stream);
            let mut resampler = LinearResampler::new(SAMPLE_RATE as f32 / in_rate);
            let mut accum: Vec<f32> = Vec::with_capacity(FRAME_SAMPLES);
            let mut encoder = opus::Encoder::new(SAMPLE_RATE, opus::Channels::Mono, opus::Application::Voip)
                .ok();
            let mut out = [0u8; 4096];
            let mut shutdown_rx = shutdown_rx;
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    Some(chunk) = mic_rx.recv() => {
                        accum.extend(resampler.process(&chunk));
                        while accum.len() >= FRAME_SAMPLES {
                            let frame: Vec<i16> = accum
                                .drain(..FRAME_SAMPLES)
                                .map(|s| (s.clamp(-1.0, 1.0) * 32767.0) as i16)
                                .collect();
                            if let Some(enc) = &mut encoder
                                && let Ok(n) = enc.encode(&frame, &mut out)
                            {
                                let opus = out[..n].to_vec();
                                let (tracks, muted) = {
                                    let inner = mgr.inner.lock().unwrap();
                                    (inner.audio_tracks.clone(), inner.muted)
                                };
                                if !muted && !tracks.is_empty() {
                                    let sample = rtc::media::Sample {
                                        data: opus.into(),
                                        timestamp: SystemInstant::now(),
                                        duration: Duration::from_millis(20),
                                        packet_timestamp: 0,
                                        prev_dropped_packets: 0,
                                        prev_padding_packets: 0,
                                    };
                                    for (_, (ssrc, track)) in &tracks {
                                        let _ = track.write_sample(*ssrc, PT_OPUS, &sample, &[]).await;
                                    }
                                }
                            }
                        }
                    }
                    else => break,
                }
            }
        });
    }

    fn ui_err(&self, msg: String) {
        let _ = self.ui_tx.send(VoiceEvent::Error(msg));
    }
}

// ------------------------------------------------------------------ handler

struct VoiceHandler {
    mgr: Arc<VoiceManager>,
    peer_id: u64,
}

#[async_trait]
impl PeerConnectionEventHandler for VoiceHandler {
    async fn on_ice_candidate(&self, event: RTCPeerConnectionIceEvent) {
        if let Ok(init) = event.candidate.to_json()
            && let Ok(data) = serde_json::to_string(&init)
        {
            self.mgr.send_signal("voice_ice", self.peer_id, &data);
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        match state {
            RTCPeerConnectionState::Failed
            | RTCPeerConnectionState::Closed
            | RTCPeerConnectionState::Disconnected => {
                self.mgr.hangup_peer(self.peer_id);
            }
            _ => {}
        }
    }

    async fn on_track(&self, track: Arc<dyn TrackRemote>) {
        let id = track.track_id().await.to_string();
        let mgr = self.mgr.clone();
        let peer_id = self.peer_id;
        if id.contains("camera") {
            spawn_remote_video(mgr, track, peer_id, VoiceVideoKind::Camera);
        } else if id.contains("screen") {
            spawn_remote_video(mgr, track, peer_id, VoiceVideoKind::Screen);
        } else {
            spawn_remote_audio(mgr, track, peer_id);
        }
    }
}

// ---------------------------------------------------------------- peer setup

async fn build_voice_peer(
    mgr: Arc<VoiceManager>,
    peer_id: u64,
) -> Result<Arc<dyn PeerConnection>, Box<dyn std::error::Error + Send + Sync>> {
    let mut media_engine = MediaEngine::default();
    media_engine.register_default_codecs()?;
    let registry = Registry::new();
    let registry = register_default_interceptors(registry, &mut media_engine)?;
    let config = RTCConfigurationBuilder::new()
        .with_ice_servers(vec![RTCIceServer {
            urls: vec!["stun:stun.l.google.com:19302".to_string()],
            ..Default::default()
        }])
        .build();
    let handler = Arc::new(VoiceHandler { mgr, peer_id });
    let runtime = default_runtime().ok_or_else(|| std::io::Error::other("no webrtc runtime"))?;
    let mut udp_addrs = vec!["127.0.0.1:0".to_string()];
    if let Some(ip) = primary_ipv4() {
        let s = format!("{ip}:0");
        if !udp_addrs.contains(&s) {
            udp_addrs.push(s);
        }
    }
    let pc = PeerConnectionBuilder::new()
        .with_configuration(config)
        .with_media_engine(media_engine)
        .with_interceptor_registry(registry)
        .with_handler(handler)
        .with_runtime(runtime)
        .with_udp_addrs(udp_addrs)
        .build()
        .await?;
    Ok(Arc::new(pc))
}

static SSRC: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);

fn gen_ssrc() -> u32 {
    let s = SSRC.fetch_add(1, Ordering::Relaxed);
    if s == 0 {
        1
    } else {
        s
    }
}

fn opus_codec() -> RTCRtpCodec {
    RTCRtpCodec {
        mime_type: "audio/opus".to_owned(),
        clock_rate: 48_000,
        channels: 2,
        sdp_fmtp_line: "minptime=10;useinbandfec=1".to_owned(),
        rtcp_feedback: vec![],
    }
}

fn h264_codec() -> RTCRtpCodec {
    RTCRtpCodec {
        mime_type: "video/H264".to_owned(),
        clock_rate: 90_000,
        channels: 0,
        sdp_fmtp_line: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42001f".to_owned(),
        rtcp_feedback: vec![],
    }
}

fn media_track(
    stream: &str,
    track_id: &str,
    kind: RtpCodecKind,
    codec: RTCRtpCodec,
) -> MediaStreamTrack {
    MediaStreamTrack::new(
        stream.to_owned(),
        track_id.to_owned(),
        track_id.to_owned(),
        kind,
        vec![RTCRtpEncodingParameters {
            rtp_coding_parameters: RTCRtpCodingParameters {
                ssrc: Some(gen_ssrc()),
                ..Default::default()
            },
            codec,
            ..Default::default()
        }],
    )
}

/// Add the microphone (Opus) + camera/screen (H.264) tracks to a peer
/// connection and remember them keyed by peer so capture tasks can write.
async fn add_local_tracks(
    mgr: &Arc<VoiceManager>,
    peer_id: u64,
    pc: Arc<dyn PeerConnection>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let audio = TrackLocalStaticSample::new(media_track(
        "feditex-audio",
        "feditex-mic",
        RtpCodecKind::Audio,
        opus_codec(),
    ))?;
    let audio_ssrc = audio.ssrcs().await.first().copied().unwrap_or(1);
    pc.add_track(Arc::new(audio.clone())).await?;
    mgr.inner
        .lock()
        .unwrap()
        .audio_tracks
        .insert(peer_id, (audio_ssrc, audio));

    for (kind, name) in [(VoiceVideoKind::Camera, "feditex-camera"), (VoiceVideoKind::Screen, "feditex-screen")] {
        let video = TrackLocalStaticSample::new(media_track(
            "feditex-video",
            name,
            RtpCodecKind::Video,
            h264_codec(),
        ))?;
        let video_ssrc = video.ssrcs().await.first().copied().unwrap_or(1);
        pc.add_track(Arc::new(video.clone())).await?;
        mgr.inner
            .lock()
            .unwrap()
            .video_tracks
            .entry(peer_id)
            .or_default()
            .insert(kind, (video_ssrc, video));
    }
    Ok(())
}

// ----------------------------------------------------------- remote decode

fn spawn_remote_audio(mgr: Arc<VoiceManager>, track: Arc<dyn TrackRemote>, peer_id: u64) {
    let handle = mgr.handle.clone();
    handle.spawn(async move {
        let mut depacketizer = rtc::rtp::codec::opus::OpusPacket::default();
        let decoder = opus::Decoder::new(SAMPLE_RATE, opus::Channels::Mono);
        let mut pcm = vec![0i16; FRAME_SAMPLES * 2];
        let mut decoder = match decoder {
            Ok(d) => d,
            Err(_) => return,
        };
        while let Some(event) = track.poll().await {
            match event {
                TrackRemoteEvent::OnRtpPacket(pkt) => {
                    let Ok(opus_payload) = depacketizer.depacketize(&pkt.payload) else {
                        continue;
                    };
                    if let Ok(n) = decoder.decode(&opus_payload[..], &mut pcm, false)
                        && n > 0
                    {
                        let samples: Vec<f32> =
                            pcm[..n].iter().map(|&s| s as f32 / 32768.0).collect();
                        let mut mix = mgr.mix.lock().unwrap();
                        let q = mix.buffers.entry(peer_id).or_default();
                        q.extend(samples);
                        while q.len() > SAMPLE_RATE as usize {
                            q.pop_front();
                        }
                    }
                }
                TrackRemoteEvent::OnEnded | TrackRemoteEvent::OnEnding => break,
                _ => {}
            }
        }
        mgr.mix.lock().unwrap().buffers.remove(&peer_id);
    });
}

fn spawn_remote_video(
    mgr: Arc<VoiceManager>,
    track: Arc<dyn TrackRemote>,
    peer_id: u64,
    kind: VoiceVideoKind,
) {
    use openh264::formats::YUVSource;
    let handle = mgr.handle.clone();
    handle.spawn(async move {
        let mut depacketizer = rtc::rtp::codec::h264::H264Packet::default();
        let mut acc: Vec<u8> = Vec::new();
        let Some(mut decoder) = openh264::decoder::Decoder::new().ok() else {
            return;
        };
        while let Some(event) = track.poll().await {
            match event {
                TrackRemoteEvent::OnRtpPacket(pkt) => {
                    let Ok(nal) = depacketizer.depacketize(&pkt.payload) else {
                        continue;
                    };
                    acc.extend_from_slice(&nal);
                    if pkt.header.marker {
                        if let Ok(Some(frame)) = decoder.decode(&acc) {
                            let (w, h) = frame.dimensions();
                            let mut rgba = vec![0u8; w * h * 4];
                            frame.write_rgba8(&mut rgba);
                            let _ = mgr.ui_tx.send(VoiceEvent::Video {
                                user_id: peer_id,
                                kind,
                                width: w as u32,
                                height: h as u32,
                                rgba,
                            });
                        }
                        acc.clear();
                    }
                }
                TrackRemoteEvent::OnEnded | TrackRemoteEvent::OnEnding => break,
                _ => {}
            }
        }
    });
}

// ----------------------------------------------------------- local capture

fn spawn_camera(mgr: Arc<VoiceManager>) {
    let stop = Arc::new(AtomicBool::new(true));
    {
        let mut inner = mgr.inner.lock().unwrap();
        if let Some(old) = inner.camera_stop.take() {
            old.store(false, Ordering::Relaxed);
        }
        inner.camera_stop = Some(stop.clone());
    }
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let handle = mgr.handle.clone();
    let writer_mgr = mgr.clone();
    handle.spawn(async move {
        while let Some(bs) = rx.recv().await {
            let tracks: Vec<(u32, TrackLocalStaticSample)> = {
                let inner = writer_mgr.inner.lock().unwrap();
                let mut out = Vec::new();
                for map in inner.video_tracks.values() {
                    if let Some((ssrc, track)) = map.get(&VoiceVideoKind::Camera) {
                        out.push((*ssrc, track.clone()));
                    }
                }
                out
            };
            if tracks.is_empty() {
                continue;
            }
            let sample = rtc::media::Sample {
                data: bs.into(),
                timestamp: SystemInstant::now(),
                duration: Duration::from_millis(66),
                packet_timestamp: 0,
                prev_dropped_packets: 0,
                prev_padding_packets: 0,
            };
            for (ssrc, track) in &tracks {
                let _ = track.write_sample(*ssrc, PT_H264, &sample, &[]).await;
            }
        }
    });
    std::thread::spawn(move || {
        let _ = run_camera_capture(mgr, stop, tx);
    });
}

fn run_camera_capture(
    _mgr: Arc<VoiceManager>,
    stop: Arc<AtomicBool>,
    tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use nokhwa::pixel_format::RgbFormat;
    use nokhwa::utils::{CameraIndex, RequestedFormat, RequestedFormatType};
    use nokhwa::Camera;
    use openh264::encoder::{Encoder, EncoderConfig, IntraFramePeriod};

    let mut cam = Camera::new(
        CameraIndex::Index(0),
        RequestedFormat::new::<RgbFormat>(RequestedFormatType::AbsoluteHighestResolution),
    )?;
    cam.open_stream()?;

    let mut encoder = Encoder::with_api_config(
        openh264::OpenH264API::from_source(),
        EncoderConfig::new()
            .bitrate(openh264::encoder::BitRate::from_bps(1_500_000))
            .intra_frame_period(IntraFramePeriod::from_num_frames(60)),
    )?;
    let mut frame_no: u64 = 0;
    while stop.load(Ordering::Relaxed) {
        let buf = match cam.frame() {
            Ok(b) => b,
            Err(_) => {
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }
        };
        let rgb = match buf.decode_image::<RgbFormat>() {
            Ok(img) => img,
            Err(_) => continue,
        };
        let (w, h) = rgb.dimensions();
        let Some((y, u, v, nw, nh)) = rgb_to_i420_scaled(rgb.as_raw(), w, h, CAMERA_MAX_WIDTH)
        else {
            continue;
        };
        if frame_no % 120 == 0 {
            encoder.force_intra_frame();
        }
        frame_no += 1;
        let src = I420Src {
            y,
            u,
            v,
            w: nw as usize,
            h: nh as usize,
            sy: stride_y(nw as usize),
            suv: stride_uv(nw as usize),
        };
        let bs = encoder.encode(&src)?;
        let mut out = Vec::new();
        bs.write_vec(&mut out);
        if tx.send(out).is_err() {
            break;
        }
        std::thread::sleep(Duration::from_millis(66));
    }
    Ok(())
}

fn spawn_screen(mgr: Arc<VoiceManager>) {
    let stop = Arc::new(AtomicBool::new(true));
    {
        let mut inner = mgr.inner.lock().unwrap();
        if let Some(old) = inner.screen_stop.take() {
            old.store(false, Ordering::Relaxed);
        }
        inner.screen_stop = Some(stop.clone());
    }
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let handle = mgr.handle.clone();
    let writer_mgr = mgr.clone();
    handle.spawn(async move {
        while let Some(bs) = rx.recv().await {
            let tracks: Vec<(u32, TrackLocalStaticSample)> = {
                let inner = writer_mgr.inner.lock().unwrap();
                let mut out = Vec::new();
                for map in inner.video_tracks.values() {
                    if let Some((ssrc, track)) = map.get(&VoiceVideoKind::Screen) {
                        out.push((*ssrc, track.clone()));
                    }
                }
                out
            };
            if tracks.is_empty() {
                continue;
            }
            let sample = rtc::media::Sample {
                data: bs.into(),
                timestamp: SystemInstant::now(),
                duration: Duration::from_millis(100),
                packet_timestamp: 0,
                prev_dropped_packets: 0,
                prev_padding_packets: 0,
            };
            for (ssrc, track) in &tracks {
                let _ = track.write_sample(*ssrc, PT_H264, &sample, &[]).await;
            }
        }
    });
    std::thread::spawn(move || {
        let _ = run_screen_capture(mgr, stop, tx);
    });
}

fn run_screen_capture(
    mgr: Arc<VoiceManager>,
    stop: Arc<AtomicBool>,
    tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use openh264::encoder::{Encoder, EncoderConfig, IntraFramePeriod};

    let monitors = xcap::Monitor::all()?;
    let Some(monitor) = monitors.first() else {
        mgr.ui_err("no monitor found for screen share".into());
        return Ok(());
    };
    let mut encoder = Encoder::with_api_config(
        openh264::OpenH264API::from_source(),
        EncoderConfig::new()
            .bitrate(openh264::encoder::BitRate::from_bps(3_000_000))
            .intra_frame_period(IntraFramePeriod::from_num_frames(30)),
    )?;
    let mut frame_no: u64 = 0;
    while stop.load(Ordering::Relaxed) {
        let img = match monitor.capture_image() {
            Ok(img) => img,
            Err(_) => {
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        let (w, h) = img.dimensions();
        let Some((y, u, v, nw, nh)) = rgba_to_i420_scaled(img.as_raw(), w, h, SCREEN_MAX_WIDTH)
        else {
            continue;
        };
        if frame_no % 30 == 0 {
            encoder.force_intra_frame();
        }
        frame_no += 1;
        let src = I420Src {
            y,
            u,
            v,
            w: nw as usize,
            h: nh as usize,
            sy: stride_y(nw as usize),
            suv: stride_uv(nw as usize),
        };
        let bs = encoder.encode(&src)?;
        let mut out = Vec::new();
        bs.write_vec(&mut out);
        if tx.send(out).is_err() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

// ---------------------------------------------------------------- utilities

fn primary_ipv4() -> Option<std::net::Ipv4Addr> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    match socket.local_addr().ok()?.ip() {
        std::net::IpAddr::V4(ip) => Some(ip),
        _ => None,
    }
}

/// Minimal streaming linear resampler. Good enough for voice (the resample
/// ratios involved are close to 1 and opus hides the rest).
struct LinearResampler {
    ratio: f64,
    pos: f64,
    last: f32,
    have_last: bool,
}

impl LinearResampler {
    fn new(ratio: f32) -> Self {
        Self { ratio: ratio as f64, pos: 0.0, last: 0.0, have_last: false }
    }

    fn process(&mut self, input: &[f32]) -> Vec<f32> {
        if input.is_empty() {
            return Vec::new();
        }
        let n = input.len() as f64;
        let out_len = ((n - self.pos.max(0.0)) * self.ratio).ceil() as usize;
        let mut out = Vec::with_capacity(out_len.min(8192));
        while self.pos < n {
            let i = self.pos.floor() as usize;
            let frac = (self.pos - i as f64) as f32;
            let s0 = if i == 0 && !self.have_last {
                input[0]
            } else if i == 0 {
                self.last
            } else {
                input[i - 1]
            };
            let s1 = input[i.min(input.len() - 1)];
            out.push(s0 * (1.0 - frac) + s1 * frac);
            self.pos += self.ratio;
        }
        self.last = *input.last().unwrap();
        self.have_last = true;
        self.pos -= n;
        out
    }
}

fn stride_y(w: usize) -> usize {
    (w + 15) & !15
}

fn stride_uv(w: usize) -> usize {
    (((w / 2) + 15) & !15).max(16)
}

struct I420Src {
    y: Vec<u8>,
    u: Vec<u8>,
    v: Vec<u8>,
    w: usize,
    h: usize,
    sy: usize,
    suv: usize,
}

impl openh264::formats::YUVSource for I420Src {
    fn dimensions(&self) -> (usize, usize) {
        (self.w, self.h)
    }
    fn strides(&self) -> (usize, usize, usize) {
        (self.sy, self.suv, self.suv)
    }
    fn y(&self) -> &[u8] {
        &self.y
    }
    fn u(&self) -> &[u8] {
        &self.u
    }
    fn v(&self) -> &[u8] {
        &self.v
    }
}

/// Nearest-neighbour downscale (if wider than `max_w`) + RGB -> I420.
fn rgb_to_i420_scaled(
    rgb: &[u8],
    w: u32,
    h: u32,
    max_w: u32,
) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>, u32, u32)> {
    pixel_to_i420(rgb, w, h, max_w, |data, i| {
        (data[i * 3], data[i * 3 + 1], data[i * 3 + 2])
    })
}

/// Nearest-neighbour downscale + RGBA -> I420.
fn rgba_to_i420_scaled(
    rgba: &[u8],
    w: u32,
    h: u32,
    max_w: u32,
) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>, u32, u32)> {
    pixel_to_i420(rgba, w, h, max_w, |data, i| {
        (data[i * 4], data[i * 4 + 1], data[i * 4 + 2])
    })
}

fn pixel_to_i420(
    src: &[u8],
    w: u32,
    h: u32,
    max_w: u32,
    pix: impl Fn(&[u8], usize) -> (u8, u8, u8),
) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>, u32, u32)> {
    if w == 0 || h == 0 {
        return None;
    }
    let scale = (max_w as f32 / w as f32).min(1.0);
    let nw = (((w as f32 * scale).round() as u32).max(2) | 1) & !1;
    let nh = (((h as f32 * scale).round() as u32).max(2) | 1) & !1;
    let sy = stride_y(nw as usize);
    let suv = stride_uv(nw as usize);
    let mut y = vec![0u8; nh as usize * sy];
    let mut u = vec![0u8; (nh as usize / 2) * suv];
    let mut v = vec![0u8; (nh as usize / 2) * suv];
    for j in 0..nh {
        let sy_idx = ((j as f32 * h as f32) / nh as f32) as u32;
        for i in 0..nw {
            let sx = ((i as f32 * w as f32) / nw as f32) as u32;
            let (r, g, b) = pix(src, (sy_idx * w + sx) as usize);
            let (rf, gf, bf) = (r as f32, g as f32, b as f32);
            let yy = 16.0 + 0.257 * rf + 0.504 * gf + 0.098 * bf;
            y[j as usize * sy + i as usize] = yy.clamp(0.0, 255.0) as u8;
            if i % 2 == 0 && j % 2 == 0 {
                let uu = 128.0 - 0.148 * rf - 0.291 * gf + 0.439 * bf;
                let vv = 128.0 + 0.439 * rf - 0.368 * gf - 0.071 * bf;
                u[(j as usize / 2) * suv + (i as usize / 2)] = uu.clamp(0.0, 255.0) as u8;
                v[(j as usize / 2) * suv + (i as usize / 2)] = vv.clamp(0.0, 255.0) as u8;
            }
        }
    }
    Some((y, u, v, nw, nh))
}

// ------------------------------------------------------------------ audio io

struct OutputCtx {
    mix: Arc<Mutex<OutputMix>>,
    rate_out: f32,
    resampler: LinearResampler,
    scratch: Vec<f32>,
}

fn fill_output<T: cpal::Sample + cpal::FromSample<f32>>(
    data: &mut [T],
    ctx: &mut OutputCtx,
    channels: usize,
) {
    if channels == 0 || data.is_empty() {
        return;
    }
    let frames = data.len() / channels;
    let needed = ((frames as f32 * SAMPLE_RATE as f32 / ctx.rate_out).ceil() as usize) + 1;
    if ctx.scratch.len() < needed {
        ctx.scratch.resize(needed, 0.0);
    }
    {
        let mut mix = ctx.mix.lock().unwrap();
        for i in 0..needed {
            let mut s = 0.0f32;
            for q in mix.buffers.values_mut() {
                if let Some(x) = q.pop_front() {
                    s += x;
                }
            }
            ctx.scratch[i] = s;
        }
    }
    let out = ctx.resampler.process(&ctx.scratch[..needed]);
    for (i, ch) in data.chunks_mut(channels).enumerate() {
        let s = out.get(i).copied().unwrap_or(0.0).clamp(-1.0, 1.0) * 0.35;
        for c in ch.iter_mut() {
            *c = T::from_sample(s);
        }
    }
}

fn pick_input_config(dev: &cpal::Device) -> Option<(cpal::StreamConfig, cpal::SampleFormat)> {
    let supported = dev.supported_input_configs().ok()?;
    for c in supported {
        if c.sample_format() == cpal::SampleFormat::F32
            && c.min_sample_rate() <= 48_000
            && c.max_sample_rate() >= 48_000
        {
            return Some((c.with_sample_rate(48_000).config(), cpal::SampleFormat::F32));
        }
    }
    None
}

fn pick_output_config(dev: &cpal::Device) -> Option<(cpal::StreamConfig, cpal::SampleFormat)> {
    let supported = dev.supported_output_configs().ok()?;
    for c in supported {
        if c.sample_format() == cpal::SampleFormat::F32
            && c.min_sample_rate() <= 48_000
            && c.max_sample_rate() >= 48_000
        {
            return Some((c.with_sample_rate(48_000).config(), cpal::SampleFormat::F32));
        }
    }
    None
}

fn build_input_stream<D>(
    dev: &cpal::Device,
    config: cpal::StreamConfig,
    format: cpal::SampleFormat,
    tx: tokio::sync::mpsc::UnboundedSender<Vec<f32>>,
    err: D,
) -> Result<cpal::Stream, cpal::Error>
where
    D: FnMut(cpal::Error) + Send + 'static,
{
    let channels = config.channels.max(1) as usize;
    match format {
        cpal::SampleFormat::F32 => {
            let cb = move |data: &[f32], _: &cpal::InputCallbackInfo| {
                send_mono_f32(data, channels, &tx);
            };
            dev.build_input_stream::<f32, _, _>(config, cb, err, None)
        }
        cpal::SampleFormat::I16 => {
            let cb = move |data: &[i16], _: &cpal::InputCallbackInfo| {
                let mono: Vec<f32> = data
                    .chunks(channels)
                    .map(|c| c.iter().map(|s| s.to_float_sample()).sum::<f32>() / channels as f32)
                    .collect();
                let _ = tx.send(mono);
            };
            dev.build_input_stream::<i16, _, _>(config, cb, err, None)
        }
        cpal::SampleFormat::U16 => {
            let cb = move |data: &[u16], _: &cpal::InputCallbackInfo| {
                let mono: Vec<f32> = data
                    .chunks(channels)
                    .map(|c| c.iter().map(|s| s.to_float_sample()).sum::<f32>() / channels as f32)
                    .collect();
                let _ = tx.send(mono);
            };
            dev.build_input_stream::<u16, _, _>(config, cb, err, None)
        }
        _ => Err(cpal::Error::new(cpal::ErrorKind::UnsupportedConfig)),
    }
}

fn send_mono_f32(data: &[f32], channels: usize, tx: &tokio::sync::mpsc::UnboundedSender<Vec<f32>>) {
    if channels == 1 {
        let _ = tx.send(data.to_vec());
    } else {
        let mono: Vec<f32> = data
            .chunks(channels)
            .map(|c| c.iter().sum::<f32>() / channels as f32)
            .collect();
        let _ = tx.send(mono);
    }
}

fn build_output_stream<D>(
    dev: &cpal::Device,
    config: cpal::StreamConfig,
    format: cpal::SampleFormat,
    cb: impl FnMut(&mut [f32]) + Send + 'static,
    err: D,
) -> Result<cpal::Stream, cpal::Error>
where
    D: FnMut(cpal::Error) + Send + 'static,
{
    match format {
        cpal::SampleFormat::F32 => {
            let mut inner = cb;
            dev.build_output_stream::<f32, _, _>(
                config,
                move |data: &mut [f32], _: &cpal::OutputCallbackInfo| inner(data),
                err,
                None,
            )
        }
        cpal::SampleFormat::I16 => {
            let mut inner = cb;
            dev.build_output_stream::<i16, _, _>(
                config,
                move |data: &mut [i16], _: &cpal::OutputCallbackInfo| {
                    let mut tmp = vec![0.0f32; data.len()];
                    inner(&mut tmp);
                    for (dst, src) in data.iter_mut().zip(tmp.iter()) {
                        *dst = i16::from_sample(*src);
                    }
                },
                err,
                None,
            )
        }
        cpal::SampleFormat::U16 => {
            let mut inner = cb;
            dev.build_output_stream::<u16, _, _>(
                config,
                move |data: &mut [u16], _: &cpal::OutputCallbackInfo| {
                    let mut tmp = vec![0.0f32; data.len()];
                    inner(&mut tmp);
                    for (dst, src) in data.iter_mut().zip(tmp.iter()) {
                        *dst = u16::from_sample(*src);
                    }
                },
                err,
                None,
            )
        }
        _ => Err(cpal::Error::new(cpal::ErrorKind::UnsupportedConfig)),
    }
}
