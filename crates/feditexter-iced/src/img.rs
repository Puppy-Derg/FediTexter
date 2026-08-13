//! Still-image helpers backed by FFmpeg (decode / resize / encode). Replaces the
//! `image` crate: any format FFmpeg understands (PNG/JPEG/WebP/GIF/AVIF/...)
//! decodes to RGBA, swscale handles resizing, and the native png/mjpeg encoders
//! produce the thumbnail/avatar payloads.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use ffmpeg_next as ffmpeg;

static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

pub struct Rgba {
    pub width: u32,
    pub height: u32,
    pub pixels: Vec<u8>,
}

/// Decode still-image bytes to RGBA. Uses a temp file for input (FFmpeg opens
/// by path). Returns the first decoded frame.
pub fn decode(bytes: &[u8]) -> Option<Rgba> {
    let mut tmp = std::env::temp_dir();
    let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
    tmp.push(format!("ft-img-{}-{}", std::process::id(), seq));
    std::fs::write(&tmp, bytes).ok()?;
    let out = decode_path(&tmp);
    let _ = std::fs::remove_file(&tmp);
    out
}

/// Decode a still image file to RGBA (first frame).
pub fn decode_path(path: &Path) -> Option<Rgba> {
    let _ = ffmpeg::init();
    let mut input = ffmpeg::format::input(path).ok()?;
    let stream_index = {
        let stream = input.streams().best(ffmpeg::media::Type::Video)?;
        stream.index()
    };
    let mut dec = {
        let stream = input.streams().find(|s| s.index() == stream_index)?;
        ffmpeg::codec::context::Context::from_parameters(stream.parameters())
            .ok()?
            .decoder()
            .video()
            .ok()?
    };
    let (w, h) = (dec.width(), dec.height());
    if w == 0 || h == 0 {
        return None;
    }
    let mut scaler = ffmpeg::software::scaling::Context::get(
        dec.format(),
        w,
        h,
        ffmpeg::format::Pixel::RGBA,
        w,
        h,
        ffmpeg::software::scaling::flag::Flags::BILINEAR,
    )
    .ok()?;
    for (s, packet) in input.packets() {
        if s.index() != stream_index {
            continue;
        }
        if dec.send_packet(&packet).is_err() {
            continue;
        }
        let mut frame = ffmpeg::frame::Video::empty();
        while dec.receive_frame(&mut frame).is_ok() {
            let mut out = ffmpeg::frame::Video::empty();
            if scaler.run(&frame, &mut out).is_ok() {
                return Some(Rgba {
                    width: out.width(),
                    height: out.height(),
                    pixels: pack_rgba(&out),
                });
            }
        }
    }
    None
}

/// True if `bytes` decode as an image.
pub fn is_image(bytes: &[u8]) -> bool {
    decode(bytes).is_some()
}

/// Resize RGBA pixels to `nw`x`nh` (Lanczos).
pub fn resize(src: &Rgba, nw: u32, nh: u32) -> Option<Rgba> {
    if nw == 0 || nh == 0 {
        return None;
    }
    let _ = ffmpeg::init();
    let mut scaler = ffmpeg::software::scaling::Context::get(
        ffmpeg::format::Pixel::RGBA,
        src.width,
        src.height,
        ffmpeg::format::Pixel::RGBA,
        nw,
        nh,
        ffmpeg::software::scaling::flag::Flags::LANCZOS,
    )
    .ok()?;
    let in_frame = frame_from_rgba(src)?;
    let mut out = ffmpeg::frame::Video::empty();
    scaler.run(&in_frame, &mut out).ok()?;
    Some(Rgba {
        width: nw,
        height: nh,
        pixels: pack_rgba(&out),
    })
}

/// Encode RGBA to a JPEG payload (ffmpeg mjpeg encoder).
pub fn encode_jpeg(src: &Rgba) -> Option<Vec<u8>> {
    let _ = ffmpeg::init();
    let in_frame = frame_from_rgba(src)?;
    let mut scaler = ffmpeg::software::scaling::Context::get(
        ffmpeg::format::Pixel::RGBA,
        src.width,
        src.height,
        ffmpeg::format::Pixel::YUVJ420P,
        src.width,
        src.height,
        ffmpeg::software::scaling::flag::Flags::BILINEAR,
    )
    .ok()?;
    let mut yuv = ffmpeg::frame::Video::empty();
    scaler.run(&in_frame, &mut yuv).ok()?;
    let codec = ffmpeg::encoder::find_by_name("mjpeg")?;
    let ctx = ffmpeg::codec::context::Context::new();
    let mut enc = ctx.encoder().video().ok()?;
    enc.set_width(src.width);
    enc.set_height(src.height);
    enc.set_format(ffmpeg::format::Pixel::YUVJ420P);
    enc.set_time_base(ffmpeg::Rational(1, 1));
    let mut encoder = enc.open_as(codec).ok()?;
    encoder.send_frame(&yuv).ok()?;
    let _ = encoder.send_eof();
    collect_packets(&mut encoder)
}

/// Encode RGBA to a PNG payload (ffmpeg png encoder).
pub fn encode_png(src: &Rgba) -> Option<Vec<u8>> {
    let _ = ffmpeg::init();
    let in_frame = frame_from_rgba(src)?;
    let codec = ffmpeg::encoder::find_by_name("png")?;
    let ctx = ffmpeg::codec::context::Context::new();
    let mut enc = ctx.encoder().video().ok()?;
    enc.set_width(src.width);
    enc.set_height(src.height);
    enc.set_format(ffmpeg::format::Pixel::RGBA);
    enc.set_time_base(ffmpeg::Rational(1, 1));
    let mut encoder = match enc.open_as(codec) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("[img] png open error: {e}");
            return None;
        }
    };
    let res = encoder.send_frame(&in_frame);
    eprintln!("[img] png send_frame: {res:?}");
    let _ = encoder.send_eof();
    collect_packets(&mut encoder)
}

fn frame_from_rgba(src: &Rgba) -> Option<ffmpeg::frame::Video> {
    let mut frame = ffmpeg::frame::Video::new(
        ffmpeg::format::Pixel::RGBA,
        src.width,
        src.height,
    );
    let stride = frame.stride(0);
    let row_bytes = src.width as usize * 4;
    for (row, chunk) in src.pixels.chunks_exact(row_bytes).enumerate() {
        let dst = &mut frame.data_mut(0)[row * stride..row * stride + row_bytes];
        dst.copy_from_slice(chunk);
    }
    Some(frame)
}

fn collect_packets(
    encoder: &mut ffmpeg::codec::encoder::Encoder,
) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut packet = ffmpeg::codec::packet::Packet::empty();
    loop {
        match encoder.receive_packet(&mut packet) {
            Ok(()) => {
                if let Some(data) = packet.data() {
                    out.extend_from_slice(data);
                }
            }
            Err(ffmpeg::Error::Eof) | Err(ffmpeg::Error::Other { .. }) => break,
            Err(_) => break,
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_and_reencodes_a_png() {
        // 2x2 RGBA pixels: red, green, blue, white.
        let pixels: Vec<u8> = vec![
            255, 0, 0, 255, 0, 255, 0, 255,
            0, 0, 255, 255, 255, 255, 255, 255,
        ];
        let src = Rgba { width: 2, height: 2, pixels };
        let png = encode_png(&src).expect("png encode");
        assert!(png.starts_with(&[0x89, b'P', b'N', b'G']));

        let decoded = decode(&png).expect("png decode");
        assert_eq!((decoded.width, decoded.height), (2, 2));
        assert_eq!(decoded.pixels, src.pixels);

        let small = resize(&decoded, 1, 1).expect("resize");
        assert_eq!((small.width, small.height), (1, 1));

        let jpeg = encode_jpeg(&decoded).expect("jpeg encode");
        assert!(jpeg.starts_with(&[0xFF, 0xD8]));
        assert!(is_image(&jpeg));
    }
}
