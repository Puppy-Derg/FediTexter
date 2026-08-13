//! FFmpeg codec wrappers for voice: Opus (real-time audio) and HEVC/H.265
//! (voice video, encoded with libx265). ffmpeg types are `!Send`, so the opus
//! encoder/decoder and the hevc decoder run on dedicated threads fed by mpsc
//! channels; the hevc encoder lives directly inside the camera/screen capture
//! threads (which are already plain threads).

use std::sync::mpsc;

use ffmpeg_next as ffmpeg;

pub const SAMPLE_RATE: u32 = 48_000;
pub const FRAME_SAMPLES: usize = 960; // 20 ms at 48 kHz

// ---------------------------------------------------------------- Opus

/// Streaming Opus encoder (mono 48 kHz f32 frames -> opus packets).
pub struct OpusEnc {
    tx: mpsc::Sender<Vec<f32>>,
    rx: mpsc::Receiver<Vec<u8>>,
}

impl OpusEnc {
    pub fn new() -> Option<Self> {
        let (tx, in_rx) = mpsc::channel::<Vec<f32>>();
        let (out_tx, rx) = mpsc::channel::<Vec<u8>>();
        std::thread::Builder::new()
            .name("voice-opus-enc".into())
            .spawn(move || {
                let _ = ffmpeg::init();
                let codec = ffmpeg::encoder::find_by_name("libopus")?;
                let ctx = ffmpeg::codec::context::Context::new();
                let mut enc = ctx.encoder().audio().ok()?;
                enc.set_rate(SAMPLE_RATE as i32);
                enc.set_format(ffmpeg::format::Sample::F32(ffmpeg::format::sample::Type::Packed));
                enc.set_channel_layout(ffmpeg::ChannelLayout::MONO);
                enc.set_time_base(ffmpeg::Rational(1, SAMPLE_RATE as i32));
                unsafe {
                    (*enc.as_mut_ptr()).frame_size = FRAME_SAMPLES as i32;
                }
                let mut encoder = enc.open_as(codec).ok()?;
                let mut pts: i64 = 0;
                while let Ok(samples) = in_rx.recv() {
                    let mut frame = ffmpeg::frame::Audio::new(
                        ffmpeg::format::Sample::F32(ffmpeg::format::sample::Type::Packed),
                        FRAME_SAMPLES,
                        ffmpeg::ChannelLayout::MONO,
                    );
                    frame.set_rate(SAMPLE_RATE);
                    {
                        let plane = frame.plane_mut::<f32>(0);
                        for (dst, src) in plane.iter_mut().zip(samples.iter().take(FRAME_SAMPLES)) {
                            *dst = *src;
                        }
                    }
                    frame.set_pts(Some(pts));
                    pts += FRAME_SAMPLES as i64;
                    if encoder.send_frame(&frame).is_err() {
                        continue;
                    }
                    let mut packet = ffmpeg::codec::packet::Packet::empty();
                    if encoder.receive_packet(&mut packet).is_ok()
                        && let Some(data) = packet.data()
                    {
                        let _ = out_tx.send(data.to_vec());
                    }
                }
                Some(())
            })
            .ok()?;
        Some(Self { tx, rx })
    }

    pub fn encode(&self, frame: &[f32]) -> Option<Vec<u8>> {
        self.tx.send(frame.to_vec()).ok()?;
        self.rx.recv_timeout(std::time::Duration::from_millis(500)).ok()
    }
}

/// Streaming Opus decoder (mono 48 kHz; opus packets -> f32 frames).
pub struct OpusDec {
    tx: mpsc::Sender<Vec<u8>>,
    rx: mpsc::Receiver<Vec<f32>>,
}

impl OpusDec {
    pub fn new() -> Option<Self> {
        let (tx, in_rx) = mpsc::channel::<Vec<u8>>();
        let (out_tx, rx) = mpsc::channel::<Vec<f32>>();
        std::thread::Builder::new()
            .name("voice-opus-dec".into())
            .spawn(move || {
                let _ = ffmpeg::init();
                let codec = ffmpeg::decoder::find_by_name("libopus")?;
                let ctx = ffmpeg::codec::context::Context::new();
                let mut dec = ctx.decoder();
                unsafe {
                    (*dec.as_mut_ptr()).sample_rate = SAMPLE_RATE as i32;
                    (*dec.as_mut_ptr()).ch_layout = ffmpeg::ChannelLayout::MONO.into();
                }
                let mut decoder = dec.open_as(codec).ok()?.audio().ok()?;
                decoder.request_format(ffmpeg::format::Sample::F32(ffmpeg::format::sample::Type::Packed));
                while let Ok(payload) = in_rx.recv() {
                    let packet = ffmpeg::codec::packet::Borrow::new(&payload);
                    if decoder.send_packet(&packet).is_err() {
                        continue;
                    }
                    let mut frame = ffmpeg::frame::Audio::empty();
                    while decoder.receive_frame(&mut frame).is_ok() {
                        // libopus decodes to packed FLT; read the interleaved
                        // buffer as f32 (mono, so one channel of samples).
                        let data = frame.data(0);
                        let samples =
                            unsafe { std::slice::from_raw_parts(data.as_ptr() as *const f32, data.len() / 4) };
                        if !samples.is_empty() {
                            let _ = out_tx.send(samples.to_vec());
                        }
                    }
                }
                Some(())
            })
            .ok()?;
        Some(Self { tx, rx })
    }

    pub fn decode(&self, payload: &[u8]) -> Option<Vec<f32>> {
        self.tx.send(payload.to_vec()).ok()?;
        self.rx.recv_timeout(std::time::Duration::from_millis(500)).ok()
    }
}

// ---------------------------------------------------------------- HEVC

/// H.265 (libx265) encoder for real-time voice video. Lives on the capture
/// thread. Input: YUV420P planes; output: an annex-B access unit.
pub struct HevcEnc {
    encoder: ffmpeg::codec::encoder::Video,
    pts: i64,
}

impl HevcEnc {
    pub fn new(
        width: u32,
        height: u32,
        fps: u32,
        keyint: u32,
        bitrate_kbps: u32,
    ) -> Option<Self> {
        let _ = ffmpeg::init();
        let codec = ffmpeg::encoder::find_by_name("libx265")?;
        let mut dict = ffmpeg::Dictionary::new();
        dict.set("preset", "ultrafast");
        dict.set("tune", "zerolatency");
        dict.set("repeat-headers", "1");
        let ctx = ffmpeg::codec::context::Context::new();
        let mut enc = ctx.encoder().video().ok()?;
        enc.set_width(width);
        enc.set_height(height);
        enc.set_format(ffmpeg::format::Pixel::YUV420P);
        enc.set_time_base(ffmpeg::Rational(1, fps.max(1) as i32));
        enc.set_frame_rate(Some(ffmpeg::Rational(fps.max(1) as i32, 1)));
        enc.set_bit_rate((bitrate_kbps as usize) * 1000);
        enc.set_gop(keyint);
        enc.set_max_b_frames(0);
        let encoder: ffmpeg::codec::encoder::Video = enc.open_as_with(codec, dict).ok()?;
        Some(Self { encoder, pts: 0 })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn encode_yuv(
        &mut self,
        y: &[u8],
        u: &[u8],
        v: &[u8],
        ystride: usize,
        uvstride: usize,
        w: u32,
        h: u32,
    ) -> Option<Vec<u8>> {
        let mut frame = ffmpeg::frame::Video::new(
            ffmpeg::format::Pixel::YUV420P,
            w,
            h,
        );
        copy_plane(&mut frame, 0, y, ystride, w as usize, h as usize);
        copy_plane(&mut frame, 1, u, uvstride, (w / 2) as usize, (h / 2) as usize);
        copy_plane(&mut frame, 2, v, uvstride, (w / 2) as usize, (h / 2) as usize);
        frame.set_pts(Some(self.pts));
        self.pts += 1;
        if self.encoder.send_frame(&frame).is_err() {
            return None;
        }
        let mut out = Vec::new();
        let mut packet = ffmpeg::codec::packet::Packet::empty();
        while self.encoder.receive_packet(&mut packet).is_ok() {
            if let Some(data) = packet.data() {
                out.extend_from_slice(data);
            }
        }
        if out.is_empty() {
            None
        } else {
            Some(out)
        }
    }

    /// Request an IDR on the next frame (recovery point for damaged streams).
    pub fn force_keyframe(&mut self) {
        let mut frame = ffmpeg::frame::Video::empty();
        frame.set_kind(ffmpeg::picture::Type::I);
        let _ = self.encoder.send_frame(&frame);
        let mut packet = ffmpeg::codec::packet::Packet::empty();
        let _ = self.encoder.receive_packet(&mut packet);
    }
}

/// Streaming HEVC decoder. Feed annex-B access units; decoded RGBA frames come
/// out on the returned receiver.
pub struct HevcDec {
    tx: mpsc::Sender<Vec<u8>>,
    rx: mpsc::Receiver<(u32, u32, Vec<u8>)>,
}

impl HevcDec {
    pub fn new() -> Option<Self> {
        let (tx, in_rx) = mpsc::channel::<Vec<u8>>();
        let (out_tx, rx) = mpsc::channel::<(u32, u32, Vec<u8>)>();
        std::thread::Builder::new()
            .name("voice-hevc-dec".into())
            .spawn(move || {
                let _ = ffmpeg::init();
                let codec = ffmpeg::decoder::find_by_name("hevc")?;
                let ctx = ffmpeg::codec::context::Context::new();
                let dec = ctx.decoder();
                let mut decoder = dec.open_as(codec).ok()?.video().ok()?;
                while let Ok(au) = in_rx.recv() {
                    let packet = ffmpeg::codec::packet::Borrow::new(&au);
                    if decoder.send_packet(&packet).is_err() {
                        continue;
                    }
                    let mut frame = ffmpeg::frame::Video::empty();
                    while decoder.receive_frame(&mut frame).is_ok() {
                        let (w, h) = (frame.width(), frame.height());
                        if let Some(rgba) = scale_to_rgba(&frame, w, h) {
                            let _ = out_tx.send((w, h, rgba));
                        }
                    }
                }
                Some(())
            })
            .ok()?;
        Some(Self { tx, rx })
    }

    pub fn feed(&self, au: Vec<u8>) -> bool {
        self.tx.send(au).is_ok()
    }

    /// Drain all decoded frames waiting in the channel.
    pub fn try_drain(&self) -> Vec<(u32, u32, Vec<u8>)> {
        let mut out = Vec::new();
        while let Ok(f) = self.rx.try_recv() {
            out.push(f);
        }
        out
    }
}

// ------------------------------------------------------------ helpers

fn scale_to_rgba(frame: &ffmpeg::frame::Video, w: u32, h: u32) -> Option<Vec<u8>> {
    let mut scaler = ffmpeg::software::scaling::Context::get(
        frame.format(),
        w,
        h,
        ffmpeg::format::Pixel::RGBA,
        w,
        h,
        ffmpeg::software::scaling::flag::Flags::BILINEAR,
    )
    .ok()?;
    let mut out = ffmpeg::frame::Video::empty();
    scaler.run(frame, &mut out).ok()?;
    let stride = out.stride(0);
    let width = out.width() as usize;
    let height = out.height() as usize;
    let data = out.data(0);
    let mut rgba = Vec::with_capacity(width * height * 4);
    for row in data.chunks_exact(stride).take(height) {
        rgba.extend_from_slice(&row[..width * 4]);
    }
    Some(rgba)
}

fn copy_plane(
    frame: &mut ffmpeg::frame::Video,
    index: usize,
    src: &[u8],
    src_stride: usize,
    width: usize,
    height: usize,
) {
    let dst_stride = frame.stride(index);
    let dst = frame.data_mut(index);
    let copy = width.min(src_stride.min(dst_stride));
    for row in 0..height {
        let s_off = row * src_stride;
        let d_off = row * dst_stride;
        if s_off + copy <= src.len() && d_off + copy <= dst.len() {
            dst[d_off..d_off + copy].copy_from_slice(&src[s_off..s_off + copy]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opus_roundtrips_audio() {
        let enc = OpusEnc::new().expect("opus encoder");
        let dec = OpusDec::new().expect("opus decoder");
        let mut opus_frames = Vec::new();
        for k in 0..4 {
            let frame: Vec<f32> = (0..FRAME_SAMPLES)
                .map(|i| ((i as f32 + k as f32 * 960.0) * 0.1).sin() * 0.5)
                .collect();
            if let Some(p) = enc.encode(&frame) {
                opus_frames.push(p);
            }
        }
        assert!(!opus_frames.is_empty(), "opus encoder produced no packets");
        let mut outs = Vec::new();
        for p in &opus_frames {
            if let Some(s) = dec.decode(p) {
                outs.push(s);
            }
        }
        assert!(!outs.is_empty(), "opus decoder produced no samples");
        // A real tone must decode to audible content (non-trivial amplitude).
        let peak: f32 = outs.iter().flatten().map(|s| s.abs()).fold(0.0, f32::max);
        assert!(peak > 0.05, "opus roundtrip peak {peak} too quiet");
    }

    #[test]
    fn hevc_roundtrips_video() {
        let (w, h) = (64u32, 48u32);
        let mut enc = HevcEnc::new(w, h, 10, 30, 500).expect("hevc encoder");
        let dec = HevcDec::new().expect("hevc decoder");
        // Gray ramp Y plane, constant chroma.
        let y: Vec<u8> = (0..(w * h) as usize).map(|i| (i % 256) as u8).collect();
        let u = vec![128u8; (w as usize / 2) * (h as usize / 2)];
        let v = vec![128u8; (w as usize / 2) * (h as usize / 2)];
        let mut aus: Vec<Vec<u8>> = Vec::new();
        for i in 0..5 {
            let bs = enc.encode_yuv(&y, &u, &v, w as usize, h as usize, w, h)
                .unwrap_or_default();
            if i == 2 {
                enc.force_keyframe();
            }
            if !bs.is_empty() {
                aus.push(bs);
            }
        }
        assert!(!aus.is_empty(), "hevc encoder produced no access units");
        // Feed the stream in order so the decoder sees headers before the
        // keyframe, then wait for the async decode thread.
        for au in &aus {
            assert!(dec.feed(au.clone()));
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut frames = Vec::new();
        while frames.is_empty() && std::time::Instant::now() < deadline {
            frames = dec.try_drain();
            if frames.is_empty() {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        }
        assert!(!frames.is_empty(), "hevc decoder produced no frames");
        let (dw, dh, rgba) = &frames[0];
        assert_eq!((*dw, *dh), (w, h));
        assert_eq!(rgba.len(), (w * h * 4) as usize);
    }
}
