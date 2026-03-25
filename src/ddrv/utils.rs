use std::sync::OnceLock;

use regex::Regex;
use reqwest::Url;

static DISCORD_CDN_RE: OnceLock<Regex> = OnceLock::new();

fn cdn_re() -> &'static Regex {
    DISCORD_CDN_RE.get_or_init(|| {
        Regex::new(r"https://cdn\.discordapp\.com/attachments/(\d+)/").unwrap()
    })
}

/// Parses a full Discord CDN attachment URL (with `ex`, `is`, `hm` query params) and
/// returns `(clean_url, ex, is, hm)` where `clean_url` has no query string.
pub fn decode_attachment_url(input_url: &str) -> (String, i64, i64, String) {
    let parsed = Url::parse(input_url)
        .unwrap_or_else(|_| panic!("decode_attachment_url: failed to parse URL: {input_url}"));

    let mut ex: i64 = 0;
    let mut is: i64 = 0;
    let mut hm = String::new();

    for (key, val) in parsed.query_pairs() {
        match key.as_ref() {
            "ex" => ex = i64::from_str_radix(&val, 16).unwrap_or(0),
            "is" => is = i64::from_str_radix(&val, 16).unwrap_or(0),
            "hm" => hm = val.into_owned(),
            _ => {}
        }
    }

    let clean = format!(
        "{}://{}{}",
        parsed.scheme(),
        parsed.host_str().unwrap_or(""),
        parsed.path()
    );

    (clean, ex, is, hm)
}

/// Rebuilds a CDN URL from a base URL (no query string) and the `ex`, `is`, `hm` values.
pub fn encode_attachment_url(base_url: &str, ex: i64, is: i64, hm: &str) -> String {
    let mut parsed = Url::parse(base_url)
        .unwrap_or_else(|_| panic!("encode_attachment_url: failed to parse URL: {base_url}"));

    parsed
        .query_pairs_mut()
        .append_pair("ex", &format!("{:x}", ex))
        .append_pair("is", &format!("{:x}", is))
        .append_pair("hm", hm);

    parsed.to_string()
}

/// Extracts the Discord channel ID from a CDN attachment URL.
pub fn extract_channel_id(url: &str) -> String {
    let caps = cdn_re()
        .captures(url)
        .unwrap_or_else(|| panic!("extract_channel_id: failed to extract channel ID from: {url}"));
    caps[1].to_string()
}
