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

/// Parse a Bluesky post URL (bsky.app or fxbsky.app) into
/// (handle, post_rkey). Returns None for anything else.
fn parse_bsky_post_url(url: &str) -> Option<(String, String)> {
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?;
    if !(host == "bsky.app" || host == "fxbsky.app" || host.ends_with(".bsky.app") || host.ends_with(".fxbsky.app")) {
        return None;
    }
    let path = parsed.path();
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let (handle, rkey): (String, String) = if host == "bsky.app" || host.ends_with(".bsky.app") {
        if segments.len() >= 4 && segments[0] == "profile" && segments[2] == "post" {
            (segments[1].to_string(), segments[3].to_string())
        } else {
            return None;
        }
    } else if segments.len() >= 3 && segments[0].starts_with('@') && segments[1] == "post" {
        // fxbsky.app/@bsky.app@bsky.social/post/{rkey} -> handle "bsky.app"
        let handle = segments[0]
            .trim_start_matches('@')
            .split('@')
            .next()
            .unwrap_or("")
            .to_string();
        (handle, segments[2].to_string())
    } else {
        return None;
    };
    Some((handle, rkey))
}

/// Fetch a Bluesky post via its public API and extract an image URL.
/// Kept separate from async futures because it's called inside async.
async fn bsky_post_image(state: &AppState, url: &str) -> Option<String> {
    let (handle, rkey) = parse_bsky_post_url(url)?;
    let uri = format!("at://{handle}/app.bsky.feed.post/{rkey}");
    let api = format!("https://public.api.bsky.app/xrpc/app.bsky.feed.getPostThread?uri={}&depth=0", urlencoding(&uri));
    let text = state
        .http
        .get(&api)
        .timeout(std::time::Duration::from_secs(6))
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()?;
    let v: Value = serde_json::from_str(&text).ok()?;
    let embed = v
        .get("thread")?
        .get("post")?
        .get("embed")?;

    let img = if let Some(images) = embed.get("images") {
        images.get(0)?.get("fullsize")?.as_str().map(str::to_string)
    } else if let Some(external) = embed.get("external") {
        external.get("thumb")?.as_str().map(str::to_string)
    } else if let Some(video) = embed.get("video") {
        video.get("thumbnail")?.as_str().map(str::to_string)
    } else if let Some(media) = embed.get("media") {
        media.get("thumbnail")?.as_str().map(str::to_string)
    } else {
        None
    }?;
    if looks_private(&img) {
        return None;
    }
    Some(img)
}

fn urlencoding(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            ':' | '/' | '#' => format!("%{:02X}", c as u32),
            c => c.to_string(),
        })
        .collect()
}

async fn bsky_post_title(state: &AppState, handle: &str, rkey: &str) -> Option<String> {
    let uri = format!("at://{handle}/app.bsky.feed.post/{rkey}");
    let api = format!("https://public.api.bsky.app/xrpc/app.bsky.feed.getPostThread?uri={}&depth=0", urlencoding(&uri));
    let text = state
        .http
        .get(&api)
        .timeout(std::time::Duration::from_secs(6))
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()?;
    let v: Value = serde_json::from_str(&text).ok()?;
    let post = v.get("thread")?.get("post")?;
    let author = post.get("author")?;
    let name = author
        .get("displayName")
        .and_then(|s| s.as_str())
        .unwrap_or_else(|| author.get("handle").and_then(|s| s.as_str()).unwrap_or(handle));
    let body = post
        .get("record")
        .and_then(|r| r.get("text"))
        .and_then(|s| s.as_str())
        .unwrap_or_default()
        .trim();
    let preview = if body.is_empty() {
        name.to_string()
    } else {
        let truncated = if body.len() > 80 { format!("{}…", &body[..80]) } else { body.to_string() };
        format!("{name}: {truncated}")
    };
    Some(preview)
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

    // fxbsky.app / bsky.app pages are JS-rendered with no og:image; the
    // actual post media lives in Bluesky's public API.
    let image = if image.is_some() {
        image
    } else if let Some(img) = bsky_post_image(&state, &url).await {
        fetch_preview_image(&state, &img).await
    } else {
        None
    };

    let title = if title.is_some() { title } else {
        // The fxbsky embed page only gives a placeholder title.
        if let Some((handle, rkey)) = parse_bsky_post_url(&url) {
            let _ = rkey;
            bsky_post_title(&state, &handle, &rkey).await.or(title)
        } else {
            title
        }
    };

    Ok(Json(json!({
        "url": url,
        "title": title,
        "description": description,
        "image": image,
    })))
}

#[cfg(test)]
mod tests {
    use super::parse_bsky_post_url;

    #[test]
    fn parses_fxbsky() {
        assert_eq!(
            parse_bsky_post_url("https://fxbsky.app/@bsky.app@bsky.social/post/3mqafridzgk2e"),
            Some(("bsky.app".to_string(), "3mqafridzgk2e".to_string()))
        );
        assert_eq!(
            parse_bsky_post_url("https://bsky.app/profile/bsky.app/post/3mqafridzgk2e"),
            Some(("bsky.app".to_string(), "3mqafridzgk2e".to_string()))
        );
        assert_eq!(
            parse_bsky_post_url("https://fxbsky.app/@bluesky@bsky.social/post/3kjt2ak2dmp26"),
            Some(("bluesky".to_string(), "3kjt2ak2dmp26".to_string()))
        );
        assert_eq!(parse_bsky_post_url("https://example.com/foo"), None);
    }
}
