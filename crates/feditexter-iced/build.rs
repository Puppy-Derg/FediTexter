//! Build-time link flags for the static FFmpeg dependency.
//!
//! libavcodec.a references x265 symbols but the archive isn't merged into it,
//! so libx265.a must be linked explicitly. x265 is C++, so the C++ runtime is
//! required too, and the png/zlib-family codecs need zlib (otherwise the
//! linker skips pulling those objects). On Windows the vcpkg crate already
//! emits every transitive dependency (including x265, zlib and their deps), so
//! nothing is needed there.

fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    // Apple's ld drops archive members whose own undefined symbols aren't yet
    // resolvable at scan time (e.g. png codec objects needing zlib that appears
    // later on the link line), so pull the whole archive in. GNU ld/LLD on
    // Linux and the vcpkg crate on Windows handle extraction fine on their own.
    if target_os == "macos"
        && let Ok(dir) = std::env::var("FFMPEG_DIR")
    {
        let libavcodec = format!("{dir}/lib/libavcodec.a");
        if std::path::Path::new(&libavcodec).exists() {
            println!("cargo:rustc-link-arg=-Wl,-force_load,{libavcodec}");
        }
    }
    match target_os.as_str() {
        "macos" => {
            println!("cargo:rustc-link-lib=x265");
            println!("cargo:rustc-link-lib=opus");
            println!("cargo:rustc-link-lib=c++");
            println!("cargo:rustc-link-lib=z");
        }
        // Emit the FFmpeg deps as raw link-args (so they land AFTER the rlibs)
        // wrapped in --start-group. `cargo:rustc-link-lib` puts them before the
        // rlibs, where GNU ld drops them under --as-needed before libavcodec's
        // objects create the references — hence "undefined reference to
        // uncompress/inflate (zlib), opus_*, x265_*" at link time.
        "linux" => {
            println!("cargo:rustc-link-arg=-Wl,--start-group");
            println!("cargo:rustc-link-arg=-Wl,-Bstatic");
            println!("cargo:rustc-link-arg=-lx265");
            println!("cargo:rustc-link-arg=-lopus");
            println!("cargo:rustc-link-arg=-lz");
            println!("cargo:rustc-link-arg=-Wl,-Bdynamic");
            println!("cargo:rustc-link-arg=-lstdc++");
            println!("cargo:rustc-link-arg=-ldl");
            println!("cargo:rustc-link-arg=-Wl,--end-group");
        }
        _ => {}
    }
}
