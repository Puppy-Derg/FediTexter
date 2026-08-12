//! P2P file transfer over WebRTC data channels.
//!
//! The server stores only metadata + a thumbnail for a file message; the actual
//! bytes travel directly from the sender to the recipient over a WebRTC data
//! channel. Signaling (fetch/offer/answer/ice/cancel) is relayed through the
//! server's WebSocket hub so we don't need STUN for discovery — but STUN is
//! still used for candidate gathering so peers behind NAT can connect.
//!
//! Flow:
//!   sender picks a file -> generates file_id + thumbnail -> sends message with
//!     metadata only (server stores no bytes) and registers the bytes for serving.
//!   recipient sees the message -> sends a `fetch` signal.
//!   sender receives `fetch` -> creates a peer connection + data channel, sends
//!     an SDP `offer`.
//!   recipient receives `offer` -> answers; both exchange trickled `ice` cands.
//!   channel opens -> sender streams the file in chunks (<=16 KiB, the SCTP
//!     receive limit of webrtc-rs 0.20), then a `{"type":"done"}` frame.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc::UnboundedSender;
use webrtc::data_channel::{DataChannel, DataChannelEvent};
use webrtc::peer_connection::{
    MediaEngine, PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler,
    RTCPeerConnectionIceEvent, RTCPeerConnectionState, RTCConfigurationBuilder, RTCIceServer,
    RTCSessionDescription, Registry, register_default_interceptors,
};
use webrtc::runtime::default_runtime;

/// A WebRTC signaling message pushed by the server over WebSocket.
#[derive(Deserialize, Clone, Debug)]
pub struct SignalEvent {
    pub file_id: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub data: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    pub from_username: Option<String>,
    #[serde(default)]
    pub from_user_id: Option<u64>,
}

/// A UI-facing event emitted by the P2P manager.
#[derive(Clone, Debug)]
pub enum P2pEvent {
    Status { file_id: String, status: String },
    Progress { file_id: String, received: u64, total: u64 },
    /// The transfer finished; `path` is the finalized file on disk.
    Complete { file_id: String, mime: String, name: String, path: std::path::PathBuf },
    Failed { file_id: String, reason: String },
}

/// The full bytes of a file we are serving to a recipient.
#[derive(Clone)]
pub struct ServingFile {
    pub file_id: String,
    pub mime: String,
    pub name: String,
    pub size: u64,
    pub bytes: Vec<u8>,
}

/// A completed transfer in progress on the receiver side.
#[derive(Clone)]
struct PeerEntry {
    pc: Arc<dyn PeerConnection>,
}

struct P2pInner {
    /// file_id -> bytes we can serve to requesters.
    serving: HashMap<String, ServingFile>,
    /// (file_id, peer_user_id) -> active peer connection (keyed per peer so a
    /// group chat can transfer the same file to several recipients at once).
    peers: HashMap<(String, u64), PeerEntry>,
    /// ICE candidates that arrived before the peer connection was registered;
    /// flushed once it is. Guards against the on_offer task not having finished
    /// building the connection when the first trickled candidates arrive.
    pending_ice: HashMap<(String, u64), Vec<webrtc::peer_connection::RTCIceCandidateInit>>,
    /// file_ids we already sent a `fetch` for (dedupe + manual-retry guard).
    fetched: HashSet<String>,
    /// file_id -> peer we asked to serve; present until the transfer actually
    /// starts, so the watchdog can mark it offline on timeout.
    pending: HashMap<String, u64>,
    /// file_ids fully received this session.
    done: HashSet<String>,
}

pub struct P2pManager {
    handle: Arc<tokio::runtime::Handle>,
    ws_tx: UnboundedSender<String>,
    ui_tx: UnboundedSender<P2pEvent>,
    inner: Mutex<P2pInner>,
}

impl std::fmt::Debug for P2pManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("P2pManager").finish_non_exhaustive()
    }
}

impl P2pManager {
    pub fn new(
        handle: tokio::runtime::Handle,
        ws_tx: UnboundedSender<String>,
        ui_tx: UnboundedSender<P2pEvent>,
    ) -> Arc<Self> {
        Arc::new(Self {
            handle: Arc::new(handle),
            ws_tx,
            ui_tx,
            inner: Mutex::new(P2pInner {
                serving: HashMap::new(),
                peers: HashMap::new(),
                pending_ice: HashMap::new(),
                fetched: HashSet::new(),
                pending: HashMap::new(),
                done: HashSet::new(),
            }),
        })
    }

    /// Register bytes for serving. Called from the UI thread when a message with
    /// an attachment is sent.
    pub fn serve(&self, file: ServingFile) {
        self.inner.lock().unwrap().serving.insert(file.file_id.clone(), file);
    }

    /// Ask `sender_id` to start serving `file_id`. Idempotent per file.
    pub fn fetch(self: &Arc<Self>, file_id: &str, sender_id: u64) {
        {
            let mut inner = self.inner.lock().unwrap();
            if inner.fetched.contains(file_id)
                || inner.done.contains(file_id)
                || inner.pending.contains_key(file_id)
            {
                return;
            }
            inner.pending.insert(file_id.to_string(), sender_id);
            inner.fetched.insert(file_id.to_string());
        }
        let _ = self
            .ui_tx
            .send(P2pEvent::Status { file_id: file_id.to_string(), status: "waiting".into() });
        self.send_signal("fetch", sender_id, file_id, None);
        eprintln!("[p2p] fetch sent for {file_id} to user {sender_id}");

        let mgr = Arc::clone(self);
        let fid = file_id.to_string();
        self.handle.spawn(async move {
            tokio::time::sleep(Duration::from_secs(120)).await;
            let mut inner = mgr.inner.lock().unwrap();
            if inner.pending.remove(&fid).is_some() {
                let _ = mgr.ui_tx.send(P2pEvent::Failed {
                    file_id: fid,
                    reason: "sender offline or connection timed out".into(),
                });
            }
        });
    }

    /// User asked for the file again (e.g. the sender was offline before).
    pub fn retry_fetch(self: &Arc<Self>, file_id: &str, sender_id: u64) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.fetched.remove(file_id);
            inner.pending.remove(file_id);
            inner.done.remove(file_id);
        }
        self.fetch(file_id, sender_id);
    }

    /// Dispatch an inbound signaling message.
    pub fn handle_signal(self: &Arc<Self>, sig: SignalEvent) {
        let peer_id = sig.from_user_id.unwrap_or(0);
        if peer_id == 0 {
            return;
        }
        match sig.kind.as_str() {
            "fetch" => self.on_fetch(sig),
            "offer" => self.on_offer(sig, peer_id),
            "answer" => self.on_answer(sig, peer_id),
            "ice" => self.on_ice(sig, peer_id),
            "cancel" => self.on_cancel(sig, peer_id),
            _ => {}
        }
    }
    fn send_signal(&self, kind: &str, to_user_id: u64, file_id: &str, data: Option<String>) {
        let _ = self.ws_tx.send(
            json!({
                "type": kind,
                "to_user_id": to_user_id,
                "file_id": file_id,
                "data": data,
            })
            .to_string(),
        );
    }

    /// Register a freshly built peer connection and flush any ICE candidates
    /// that arrived for it before registration completed.
    fn register_peer(&self, file_id: &str, peer_id: u64, pc: Arc<dyn PeerConnection>) {
        let mut inner = self.inner.lock().unwrap();
        inner.peers.insert((file_id.to_string(), peer_id), PeerEntry { pc: pc.clone() });
        let pending = inner
            .pending_ice
            .remove(&(file_id.to_string(), peer_id))
            .unwrap_or_default();
        drop(inner);
        for init in pending {
            let pc = pc.clone();
            self.handle.spawn(async move {
                let _ = pc.add_ice_candidate(init).await;
            });
        }
    }

    fn on_fetch(self: &Arc<Self>, sig: SignalEvent) {
        let peer_id = sig.from_user_id.unwrap_or(0);
        let Some(file) = self.inner.lock().unwrap().serving.get(&sig.file_id).cloned() else {
            // We no longer hold the file (e.g. restarted since sending).
            self.send_signal("cancel", peer_id, &sig.file_id, None);
            return;
        };
        let mgr = Arc::clone(self);
        let file_id = sig.file_id.clone();
        self.handle.spawn(async move {
            let result = async {
                let pc = build_peer(mgr.clone(), file_id.clone(), peer_id).await?;
                let dc = pc.create_data_channel("feditex", None).await?;
                mgr.register_peer(&file_id, peer_id, pc.clone());
                let offer = pc.create_offer(None).await?;
                pc.set_local_description(offer.clone()).await?;
                let sdp = serde_json::to_string(&offer)?;
                mgr.send_signal("offer", peer_id, &file_id, Some(sdp));

                let mgr2 = Arc::clone(&mgr);
                let fid2 = file_id.clone();
                mgr.handle.spawn(async move {
                    mgr2.run_sender(dc, fid2, peer_id, file).await;
                });
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }
            .await;
            if let Err(e) = result {
                eprintln!("[p2p] offer failed: {e}");
                mgr.fail_file(&file_id, "could not establish connection");
            }
        });
    }

    fn on_offer(self: &Arc<Self>, sig: SignalEvent, peer_id: u64) {
        let Ok(sdp) = serde_json::from_str::<RTCSessionDescription>(
            sig.data.as_deref().unwrap_or_default(),
        ) else {
            return;
        };
        let mgr = Arc::clone(self);
        let file_id = sig.file_id.clone();
        self.handle.spawn(async move {
            let result = async {
                let pc = build_peer(mgr.clone(), file_id.clone(), peer_id).await?;
                pc.set_remote_description(sdp.clone()).await?;
                let answer = pc.create_answer(None).await?;
                pc.set_local_description(answer.clone()).await?;
                mgr.register_peer(&file_id, peer_id, pc.clone());
                let sdp = serde_json::to_string(&answer)?;
                mgr.send_signal("answer", peer_id, &file_id, Some(sdp));
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }
            .await;
            if let Err(e) = result {
                eprintln!("[p2p] answer failed: {e}");
                mgr.fail_file(&file_id, "could not establish connection");
            }
        });
    }

    fn on_answer(&self, sig: SignalEvent, peer_id: u64) {
        let Ok(sdp) = serde_json::from_str::<RTCSessionDescription>(
            sig.data.as_deref().unwrap_or_default(),
        ) else {
            return;
        };
        let Some(entry) = self.inner.lock().unwrap().peers.get(&(sig.file_id.clone(), peer_id)).cloned() else {
            return;
        };
        let pc = entry.pc;
        self.handle.spawn(async move {
            let _ = pc.set_remote_description(sdp).await;
        });
    }

    fn on_ice(&self, sig: SignalEvent, peer_id: u64) {
        let Ok(init) = serde_json::from_str::<webrtc::peer_connection::RTCIceCandidateInit>(
            sig.data.as_deref().unwrap_or_default(),
        ) else {
            return;
        };
        let pc = {
            let mut inner = self.inner.lock().unwrap();
            match inner.peers.get(&(sig.file_id.clone(), peer_id)).cloned() {
                Some(entry) => Some(entry.pc),
                None => {
                    inner
                        .pending_ice
                        .entry((sig.file_id.clone(), peer_id))
                        .or_default()
                        .push(init.clone());
                    None
                }
            }
        };
        if let Some(pc) = pc {
            self.handle.spawn(async move {
                let _ = pc.add_ice_candidate(init).await;
            });
        }
    }

    fn on_cancel(&self, sig: SignalEvent, peer_id: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.pending.remove(&sig.file_id);
        if let Some(entry) = inner.peers.remove(&(sig.file_id.clone(), peer_id)) {
            let pc = entry.pc;
            self.handle.spawn(async move {
                let _ = pc.close().await;
            });
        }
        drop(inner);
        let _ = self.ui_tx.send(P2pEvent::Failed {
            file_id: sig.file_id.clone(),
            reason: "sender is no longer online".into(),
        });
    }

    /// Called by the handler once the connection actually establishes.
    fn transfer_started(&self, file_id: &str) {
        self.inner.lock().unwrap().pending.remove(file_id);
        let _ = self
            .ui_tx
            .send(P2pEvent::Status { file_id: file_id.to_string(), status: "connecting".into() });
    }

    fn fail_file(&self, file_id: &str, reason: &str) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.pending.remove(file_id);
            let peers: Vec<_> = inner
                .peers
                .keys()
                .filter(|(f, _)| f == file_id)
                .cloned()
                .collect();
            for key in peers {
                if let Some(entry) = inner.peers.remove(&key) {
                    let pc = entry.pc;
                    self.handle.spawn(async move {
                        let _ = pc.close().await;
                    });
                }
            }
        }
        let _ = self
            .ui_tx
            .send(P2pEvent::Failed { file_id: file_id.to_string(), reason: reason.to_string() });
    }

    fn finish_peer(&self, file_id: &str, peer_id: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.pending.remove(file_id);
        if let Some(entry) = inner.peers.remove(&(file_id.to_string(), peer_id)) {
            let pc = entry.pc;
            self.handle.spawn(async move {
                let _ = pc.close().await;
            });
        }
    }

    /// Sender side: wait for the channel to open, stream the file, then wait for
    /// the receiver's ack before tearing the connection down (so queued SCTP
    /// data is actually flushed).
    async fn run_sender(&self, dc: Arc<dyn DataChannel>, file_id: String, peer_id: u64, file: ServingFile) {
        while let Some(event) = dc.poll().await {
            match event {
                DataChannelEvent::OnOpen => {
                    eprintln!("[p2p] sender channel open for {file_id}, streaming {} bytes", file.size);
                    let _ = self.ui_tx.send(P2pEvent::Status {
                        file_id: file_id.clone(),
                        status: "sending".into(),
                    });
                    let control = json!({
                        "type": "file",
                        "file_id": file_id,
                        "size": file.size,
                        "name": file.name,
                        "mime": file.mime,
                    })
                    .to_string();
                    if dc.send_text(&control).await.is_err() {
                        break;
                    }
                    // webrtc-rs 0.20's SCTP receive path caps messages at 16 KiB;
                    // use 8 KiB chunks to stay well under the limit.
                    const CHUNK: usize = 8 * 1024;
                    let mut ok = true;
                    for chunk in file.bytes.chunks(CHUNK) {
                        if dc.send(bytes::BytesMut::from(chunk)).await.is_err() {
                            ok = false;
                            break;
                        }
                    }
                    if ok {
                        let _ = dc.send_text(r#"{"type":"done"}"#).await;
                    }
                    break;
                }
                DataChannelEvent::OnClose | DataChannelEvent::OnError => break,
                _ => {}
            }
        }
        // Wait for the receiver's ack before closing the peer connection.
        let timeout = tokio::time::sleep(Duration::from_secs(30));
        tokio::pin!(timeout);
        loop {
            tokio::select! {
                _ = &mut timeout => break,
                event = dc.poll() => match event {
                    Some(DataChannelEvent::OnMessage(msg)) if msg.is_string => {
                        if String::from_utf8_lossy(&msg.data).contains("ack") {
                            break;
                        }
                    }
                    Some(DataChannelEvent::OnClose) | Some(DataChannelEvent::OnError) => break,
                    Some(_) => {}
                    None => break,
                }
            }
        }
        self.finish_peer(&file_id, peer_id);
    }

    /// Receiver side: read control/chunk/done frames off the channel, streaming
    /// the bytes straight to disk so large transfers don't accumulate in RAM.
    async fn run_receiver(&self, dc: Arc<dyn DataChannel>, file_id: &str, peer_id: u64) {
        let mut download: Option<(String, u64, String, String, std::fs::File, u64, std::path::PathBuf)> = None;
        while let Some(event) = dc.poll().await {
            match event {
                DataChannelEvent::OnMessage(msg) => {
                    if msg.is_string {
                        let text = String::from_utf8_lossy(&msg.data);
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                            match v.get("type").and_then(|t| t.as_str()) {
                                Some("file") => {
                                    let fid = v.get("file_id").and_then(|x| x.as_str()).unwrap_or("").to_string();
                                    let size = v.get("size").and_then(|x| x.as_u64()).unwrap_or(0);
                                    let name = v.get("name").and_then(|x| x.as_str()).unwrap_or("file").to_string();
                                    let mime = v.get("mime").and_then(|x| x.as_str()).unwrap_or("application/octet-stream").to_string();
                                    // Open a temp file in the cache dir to stream into.
                                    let dir = cache_dir();
                                    let _ = std::fs::create_dir_all(&dir);
                                    let tmp = dir.join(format!("{fid}.part"));
                                    let file = std::fs::File::create(&tmp).ok();
                                    download = file.map(|f| (fid, size, name, mime, f, 0, tmp));
                                    if let Some((fid, _, _, _, _, _, _)) = &download {
                                        let _ = self.ui_tx.send(P2pEvent::Status {
                                            file_id: fid.clone(),
                                            status: "downloading".into(),
                                        });
                                    }
                                }
                                Some("done") => {
                                    if let Some((fid, _expected, name, mime, file, received, tmp)) = download.take() {
                                        // Flush and finalize: rename the .part file to its
                                        // final cache path (keyed by file_id).
                                        let _ = file.sync_all();
                                        drop(file);
                                        let final_path = cache_dir().join(&fid);
                                        let _ = std::fs::rename(&tmp, &final_path);
                                        self.complete(&fid);
                                        let _ = self.ui_tx.send(P2pEvent::Complete {
                                            file_id: fid,
                                            mime,
                                            name,
                                            path: final_path,
                                        });
                                        let _ = received;
                                        // Tell the sender the transfer finished so it can
                                        // tear down without dropping in-flight data.
                                        let _ = dc.send_text(r#"{"type":"ack"}"#).await;
                                    }
                                    // Wait for the sender to close after it receives the
                                    // ack; only then is it safe to close our side.
                                    let grace = tokio::time::sleep(Duration::from_secs(10));
                                    tokio::pin!(grace);
                                    loop {
                                        tokio::select! {
                                            _ = &mut grace => break,
                                            event = dc.poll() => match event {
                                                Some(DataChannelEvent::OnClose)
                                                | Some(DataChannelEvent::OnError) => break,
                                                Some(_) => {}
                                                None => break,
                                            }
                                        }
                                    }
                                    break;
                                }
                                _ => {}
                            }
                        }
                    } else if let Some((fid, expected, _name, _mime, file, last_emitted, _tmp)) = download.as_mut() {
                        use std::io::Write;
                        if file.write_all(&msg.data).is_err() {
                            break;
                        }
                        let received = *last_emitted + msg.data.len() as u64;
                        *last_emitted = received;
                        // Throttle progress events to ~once per 512 KiB.
                        if received % (512 * 1024) < msg.data.len() as u64 {
                            let _ = self.ui_tx.send(P2pEvent::Progress {
                                file_id: fid.clone(),
                                received,
                                total: *expected,
                            });
                        }
                    }
                }
                DataChannelEvent::OnClose | DataChannelEvent::OnError => break,
                _ => {}
            }
        }
        self.finish_peer(file_id, peer_id);
        // Clean up a partial .part file if the transfer was interrupted.
        if let Some((fid, _, _, _, _file, _received, tmp)) = download.take() {
            let _ = std::fs::remove_file(&tmp);
            let _ = self.ui_tx.send(P2pEvent::Failed {
                file_id: fid,
                reason: "transfer interrupted".into(),
            });
        }
    }

    fn complete(&self, file_id: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.pending.remove(file_id);
        inner.done.insert(file_id.to_string());
    }
}

/// Connection state handler: relays ICE candidates and surfaces failures.
struct PcHandler {
    mgr: Arc<P2pManager>,
    file_id: String,
    peer_id: u64,
}

#[async_trait]
impl PeerConnectionEventHandler for PcHandler {
    async fn on_ice_candidate(&self, event: RTCPeerConnectionIceEvent) {
        if let Ok(init) = event.candidate.to_json()
            && let Ok(data) = serde_json::to_string(&init)
        {
            self.mgr.send_signal("ice", self.peer_id, &self.file_id, Some(data));
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        eprintln!("[p2p] connection state for {}: {state:?}", self.file_id);
        match state {
            RTCPeerConnectionState::Connected => {
                self.mgr.transfer_started(&self.file_id);
            }
            RTCPeerConnectionState::Failed => {
                self.mgr.fail_file(&self.file_id, "connection failed");
            }
            _ => {}
        }
    }

    async fn on_data_channel(&self, data_channel: Arc<dyn DataChannel>) {
        eprintln!("[p2p] data channel received for {}", self.file_id);
        let mgr = Arc::clone(&self.mgr);
        let handle = Arc::clone(&mgr.handle);
        let file_id = self.file_id.clone();
        let peer_id = self.peer_id;
        handle.spawn(async move {
            mgr.run_receiver(data_channel, &file_id, peer_id).await;
        });
    }
}

async fn build_peer(
    mgr: Arc<P2pManager>,
    file_id: String,
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
    let handler = Arc::new(PcHandler {
        mgr,
        file_id,
        peer_id,
    });
    let runtime = default_runtime().ok_or_else(|| std::io::Error::other("no webrtc runtime"))?;
    // Host candidates are derived from the bound socket addresses (the gatherer
    // does not enumerate interfaces), so binding 0.0.0.0 yields unusable
    // `0.0.0.0` candidates. Bind loopback plus the machine's primary outbound
    // IPv4 (found without sending packets via the classic UDP-connect trick).
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

/// The machine's primary outbound IPv4 address. `connect()` on a UDP socket does
/// not transmit anything — it only selects a route and lets `local_addr()` report
/// the address a real connection to that destination would use.
/// The on-disk directory where received P2P files land (final cache) and where
/// in-progress `.part` files are streamed.
fn cache_dir() -> std::path::PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    std::path::Path::new(&home).join(".feditexter_files")
}

fn primary_ipv4() -> Option<std::net::Ipv4Addr> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    match socket.local_addr().ok()?.ip() {
        std::net::IpAddr::V4(ip) => Some(ip),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use tokio::sync::mpsc::unbounded_channel;

    /// Wire two P2pManagers together through an in-process signaling "bus" that
    /// mimics the server's WebSocket hub, then transfer a file end to end.
    #[test]
    fn p2p_transfer_end_to_end() {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let handle = rt.handle().clone();

        let (a_ws_tx, mut a_ws_rx) = unbounded_channel();
        let (a_ui_tx, _a_ui_rx) = unbounded_channel();
        let (b_ws_tx, mut b_ws_rx) = unbounded_channel();
        let (b_ui_tx, mut b_ui_rx) = unbounded_channel();

        let a = P2pManager::new(handle.clone(), a_ws_tx, a_ui_tx);
        let b = P2pManager::new(handle.clone(), b_ws_tx, b_ui_tx);

        let a2 = Arc::clone(&a);
        let b2 = Arc::clone(&b);
        handle.spawn(async move {
            while let Some(text) = a_ws_rx.recv().await {
                let v: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(v["to_user_id"].as_u64().unwrap(), 2);
                b2.handle_signal(SignalEvent {
                    file_id: v["file_id"].as_str().unwrap().to_string(),
                    kind: v["type"].as_str().unwrap().to_string(),
                    data: v["data"].as_str().map(|s| s.to_string()),
                    from_username: None,
                    from_user_id: Some(1),
                });
            }
        });
        handle.spawn(async move {
            while let Some(text) = b_ws_rx.recv().await {
                let v: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(v["to_user_id"].as_u64().unwrap(), 1);
                a2.handle_signal(SignalEvent {
                    file_id: v["file_id"].as_str().unwrap().to_string(),
                    kind: v["type"].as_str().unwrap().to_string(),
                    data: v["data"].as_str().map(|s| s.to_string()),
                    from_username: None,
                    from_user_id: Some(2),
                });
            }
        });

        let payload: Vec<u8> = (0..2_000_000u32).map(|i| (i % 251) as u8).collect();
        a.serve(ServingFile {
            file_id: "testfile-1".into(),
            mime: "application/octet-stream".into(),
            name: "blob.bin".into(),
            size: payload.len() as u64,
            bytes: payload.clone(),
        });
        b.fetch("testfile-1", 1);

        rt.block_on(async {
            let timeout = tokio::time::sleep(Duration::from_secs(90));
            tokio::pin!(timeout);
            let (file_id, path) = loop {
                tokio::select! {
                    _ = &mut timeout => panic!("transfer timed out"),
                    msg = b_ui_rx.recv() => match msg {
                        Some(P2pEvent::Complete { file_id, path, .. }) => break (file_id, path),
                        Some(_) => continue,
                        None => panic!("receiver ui channel closed"),
                    }
                }
            };
            assert_eq!(file_id, "testfile-1");
            let bytes = std::fs::read(&path).expect("completed file should exist on disk");
            assert_eq!(bytes.len(), payload.len());
            assert_eq!(bytes, payload);
        });
    }
}
