#!/usr/bin/env bash
# Build a minimal, statically-linked FFmpeg for linking into the FediTexter
# iced client. Produces include/ and lib/ (static archives) under PREFIX.
#
# Why from source: prebuilt static libraries for the latest stable FFmpeg
# (9.0.x) do not exist yet across the whole release matrix. We only need the
# core decoding libraries (avcodec/avformat/swscale/swresample), so autodetect
# is disabled to avoid external dependencies entirely.
#
# Usage: build-ffmpeg.sh <prefix> [jobs]
set -euo pipefail

PREFIX="${1:?usage: build-ffmpeg.sh <prefix> [jobs]}"
JOBS="${2:-$(getconf _NPROCESSORS_ONLN)}"
FFMPEG_TAG="${FFMPEG_TAG:-n9.0.1}"

SRC_DIR="$PREFIX/ffmpeg-src"

if [ ! -d "$SRC_DIR/.git" ]; then
  git clone --depth 1 --branch "$FFMPEG_TAG" \
    https://github.com/FFmpeg/FFmpeg.git "$SRC_DIR"
else
  git -C "$SRC_DIR" fetch --depth 1 origin tag "$FFMPEG_TAG"
  git -C "$SRC_DIR" checkout -q "$FFMPEG_TAG"
fi

cd "$SRC_DIR"

./configure \
  --prefix="$PREFIX" \
  --disable-shared \
  --enable-static \
  --enable-pic \
  --disable-autodetect \
  --disable-programs \
  --disable-doc \
  --disable-network \
  --disable-avdevice \
  --disable-avfilter \
  --disable-encoders \
  --disable-muxers \
  --disable-bsfs \
  --disable-devices \
  --disable-debug

make -j"$JOBS"
make install
