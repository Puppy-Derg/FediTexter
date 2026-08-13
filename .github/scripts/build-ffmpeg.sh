#!/usr/bin/env bash
# Build a minimal, statically-linked FFmpeg for linking into the FediTexter
# iced client. Produces include/ and lib/ (static archives) under PREFIX.
#
# Why from source: prebuilt static libraries for the latest stable FFmpeg
# (9.0.x) do not exist yet across the whole release matrix. The build links
# libx265 (GPL) for real-time HEVC voice-video encoding and keeps everything
# else native (opus, png/mjpeg encoders, all decoders), so autodetect is
# disabled to avoid unneeded external dependencies.
#
# Requirements: git, make, a C compiler, cmake, and nasm (x86_64 only).
#
# Usage: build-ffmpeg.sh <prefix> [jobs]
set -euo pipefail

PREFIX="${1:?usage: build-ffmpeg.sh <prefix> [jobs]}"
JOBS="${2:-$(getconf _NPROCESSORS_ONLN)}"
FFMPEG_TAG="${FFMPEG_TAG:-n9.0.1}"
X265_TAG="${X265_TAG:-4.2}"
OPUS_TAG="${OPUS_TAG:-v1.6.1}"

# x265 is C++; the FFmpeg link tests and EXTRALIBS need the C++ runtime.
case "$(uname -s)" in
  Darwin) CXX_LIBS="-lc++" ;;
  Linux) CXX_LIBS="-lstdc++" ;;
  *) CXX_LIBS="" ;;
esac

SRC_DIR="$PREFIX/ffmpeg-src"
X265_DIR="$PREFIX/x265-src"
OPUS_DIR="$PREFIX/libopus-src"

# ----------------------------------------------------------------- libopus
# Opus codec for real-time voice audio (libopus, not the experimental native
# encoder). Built before FFmpeg so configure can find it.
if [ ! -d "$OPUS_DIR/.git" ]; then
  git clone --depth 1 --branch "$OPUS_TAG" \
    https://github.com/xiph/opus.git "$OPUS_DIR"
else
  git -C "$OPUS_DIR" fetch --depth 1 origin tag "$OPUS_TAG"
  git -C "$OPUS_DIR" checkout -q "$OPUS_TAG"
fi

if [ ! -f "$PREFIX/lib/libopus.a" ]; then
  cmake -S "$OPUS_DIR" -B "$OPUS_DIR/build" \
    -DCMAKE_INSTALL_PREFIX="$PREFIX" \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=OFF \
    -DOPUS_BUILD_PROGRAMS=OFF \
    -DOPUS_BUILD_TESTING=OFF
  cmake --build "$OPUS_DIR/build" -j"$JOBS"
  cmake --install "$OPUS_DIR/build"
fi

# ---------------------------------------------------------------- libx265
# HEVC encoder (GPL). Built before FFmpeg so configure can find it.
if [ ! -d "$X265_DIR/.git" ]; then
  git clone --depth 1 --branch "$X265_TAG" \
    https://bitbucket.org/multicoreware/x265_git.git "$X265_DIR"
else
  git -C "$X265_DIR" fetch --depth 1 origin tag "$X265_TAG"
  git -C "$X265_DIR" checkout -q "$X265_TAG"
fi

if [ ! -f "$PREFIX/lib/libx265.a" ]; then
  cmake -S "$X265_DIR/source" -B "$X265_DIR/build" \
    -DCMAKE_INSTALL_PREFIX="$PREFIX" \
    -DCMAKE_BUILD_TYPE=Release \
    -DENABLE_SHARED=OFF \
    -DENABLE_CLI=OFF \
    -DENABLE_PIC=ON
  cmake --build "$X265_DIR/build" -j"$JOBS"
  cmake --install "$X265_DIR/build"
fi

# ------------------------------------------------------------------ FFmpeg
if [ ! -d "$SRC_DIR/.git" ]; then
  git clone --depth 1 --branch "$FFMPEG_TAG" \
    https://github.com/FFmpeg/FFmpeg.git "$SRC_DIR"
else
  git -C "$SRC_DIR" fetch --depth 1 origin tag "$FFMPEG_TAG"
  git -C "$SRC_DIR" checkout -q "$FFMPEG_TAG"
fi

cd "$SRC_DIR"

PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig" \
./configure \
  --prefix="$PREFIX" \
  --disable-shared \
  --enable-static \
  --enable-pic \
  --disable-autodetect \
  --enable-gpl \
  --enable-libx265 \
  --enable-libopus \
  --enable-zlib \
  --disable-programs \
  --disable-doc \
  --disable-network \
  --disable-avdevice \
  --disable-avfilter \
  --disable-muxers \
  --disable-bsfs \
  --disable-devices \
  --disable-debug \
  --extra-cflags="-I$PREFIX/include" \
  --extra-ldflags="-L$PREFIX/lib" \
  --extra-libs="$CXX_LIBS"

make -j"$JOBS"
make install
