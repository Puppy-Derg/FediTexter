use axum::extract::State;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use scraper::{Html, Selector};

use crate::api::error::ApiError;
use crate::db::AppState;

#[derive(Deserialize)]
pub struct PreviewRequest {
    pub url: String,
}

const MAX_PAGE_BYTES: usize = 2_000_000;
const MAX_IMAGE_BYTES: usize = 3_000_000;
const IMAGE_MAX_DIM: u32 = 1024;

/// Basic SSRF guard: reject requests to private/loopback/link-local hosts.
fn looks_private(url: &str) -> bool {
    let host = url
        .split("://")
        .nth(1)
        .unwrap_or(url)
        .split('/')
        .next()
        .unwrap_or("")
        .trim_start_matches('[')
        .split(']')
        .next()
        .unwrap_or("")
        .split(':')
        .next()
        .unwrap_or("");
    if host.is_empty() || host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<std::net::IpAddr>().is_ok_and(|ip| match ip {
        std::net::IpAddr::V4(v4) => v4.is_loopback() || v4.is_private() || v4.is_link_local(),
        std::net::IpAddr::V6(v6) => {
            v6.is_loopback() || v6.is_unspecified() || v6.is_unicast_link_local()
        }
    })
}

async fn fetch_preview_image(state: &AppState, img: &str) -> Option<String> {
    let bytes = state
        .http
        .get(img)
        .header("User-Agent", "FediTexterBot/1.0")
        .timeout(std::time::Duration::from_secs(6))
        .send()
        .await
        .ok()?
        .bytes()
        .await
        .ok()?;
    if bytes.len() > MAX_IMAGE_BYTES {
        return None;
    }
    let decoded = image::load_from_memory(&bytes).ok()?;
    let (w, h) = (decoded.width(), decoded.height());
    let scaled = if w > IMAGE_MAX_DIM || h > IMAGE_MAX_DIM {
        let scale = IMAGE_MAX_DIM as f32 / w.max(h) as f32;
        decoded.resize(
            ((w as f32) * scale) as u32,
            ((h as f32) * scale) as u32,
            image::imageops::FilterType::Lanczos3,
        )
    } else {
        decoded
    };
    let mut out = std::io::Cursor::new(Vec::new());
    scaled.write_to(&mut out, image::ImageFormat::Jpeg).ok()?;
    use base64::Engine;
    let data = base64::engine::general_purpose::STANDARD.encode(out.into_inner());
    Some(format!("data:image/jpeg;base64,{data}"))
}

fn meta_content(dom: &Html, selector: &Selector, prop: &str) -> Option<String> {
    dom.select(selector).find_map(|m| {
        let property = m.value().attr("property").unwrap_or("");
        let name = m.value().attr("name").unwrap_or("");
        if property == prop || name == prop {
            m.value().attr("content").map(|s| s.trim().to_string())
        } else {
            None
        }
    })
}

/// Synchronously extract title / description / image URL from HTML.
/// `scraper::Html` is not `Sync`, so this keeps it out of the async future.
fn parse_preview(base_url: &str, html: &str) -> (Option<String>, Option<String>, Option<String>) {
    let dom = Html::parse_document(html);
    let title_sel = Selector::parse("title").unwrap();
    let meta_sel = Selector::parse("meta").unwrap();

    let title = meta_content(&dom, &meta_sel, "og:title")
        .or_else(|| meta_content(&dom, &meta_sel, "twitter:title"))
        .or_else(|| {
            dom.select(&title_sel).next().map(|t| {
                t.text().collect::<String>().trim().to_string()
            })
        })
        .filter(|s| !s.is_empty());

    let description = meta_content(&dom, &meta_sel, "og:description")
        .or_else(|| meta_content(&dom, &meta_sel, "twitter:description"))
        .filter(|s| !s.is_empty());

    // Many pages use protocol-relative (`//cdn/…`) or absolute-path (`/img/…`)
    // og:image URLs; resolve them against the page's own URL.
    let image_url = meta_content(&dom, &meta_sel, "og:image")
        .or_else(|| meta_content(&dom, &meta_sel, "twitter:image"))
        .and_then(|raw| {
            if raw.starts_with("http://") || raw.starts_with("https://") {
                return (!looks_private(&raw)).then_some(raw);
            }
            let resolved = url::Url::parse(base_url)
                .ok()
                .and_then(|base| base.join(&raw).ok())
                .map(|u| u.to_string());
            resolved
                .filter(|u| u.starts_with("http") && !looks_private(u))
        });

    (title, description, image_url)
}

pub async fn link_preview(
    State(state): State<AppState>,
    Json(body): Json<PreviewRequest>,
) -> Result<Json<Value>, ApiError> {
    let url = body.url.trim().to_string();
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(ApiError::BadRequest("url must be http or https"));
    }
    if looks_private(&url) {
        return Err(ApiError::BadRequest("url not allowed"));
    }

    let html = state
        .http
        .get(&url)
        .header("User-Agent", "FediTexterBot/1.0 (+https://dergdungeon.com.au)")
        .timeout(std::time::Duration::from_secs(8))
        .send()
        .await
        .map_err(|_| ApiError::BadRequest("could not fetch url"))?
        .bytes()
        .await
        .map_err(|_| ApiError::BadRequest("could not read response"))?;

    if html.len() > MAX_PAGE_BYTES {
        return Err(ApiError::BadRequest("page too large"));
    }

    let (title, description, image_url) = parse_preview(&url, &String::from_utf8_lossy(&html));

    let image = match image_url {
        Some(img) => fetch_preview_image(&state, &img).await,
        None => None,
    };

    Ok(Json(json!({
        "url": url,
        "title": title,
        "description": description,
        "image": image,
    })))
}
