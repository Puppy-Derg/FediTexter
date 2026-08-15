# FediTexter build helpers.
#
# `make client` (and `make client-release`) build a statically-linked FFmpeg
# from source for the CURRENT rust target (see build-ffmpeg.sh for why), then
# compile the iced client against it. The FFmpeg prefix is keyed by the target
# triple so different targets never share a build dir.
#
# Requirements: git, make, a C compiler, cmake, and nasm (x86_64 only).
#
# TARGET defaults to the host triple but can be overridden, e.g.
#   make client-release TARGET=x86_64-unknown-linux-gnu
# (Note: cross-compiling FFmpeg itself needs a matching cross toolchain — the
# CI matrix uses native runners, so host == target there.)

TARGET ?= $(shell rustc -vV | sed -n 's/^host: //p')
FFMPEG_PREFIX := target/ffmpeg/$(TARGET)
FFMPEG_DIR := $(abspath $(FFMPEG_PREFIX))
FFMPEG_SCRIPT := .github/scripts/build-ffmpeg.sh
JOBS := $(shell getconf _NPROCESSORS_ONLN)

.PHONY: all client client-release server ffmpeg ffmpeg-clean clean

all: client

## Build the iced client for TARGET (builds FFmpeg first).
client: $(FFMPEG_PREFIX)/lib/libavcodec.a
	FFMPEG_DIR=$(FFMPEG_DIR) cargo build -p feditexter-iced --target $(TARGET)

## Same as `client` but with a release build.
client-release: $(FFMPEG_PREFIX)/lib/libavcodec.a
	FFMPEG_DIR=$(FFMPEG_DIR) cargo build --release -p feditexter-iced --target $(TARGET)

## Build the server (no FFmpeg needed).
server:
	cargo build -p feditexter-server

## Build (or fetch/update) FFmpeg for the current target.
ffmpeg: $(FFMPEG_PREFIX)/lib/libavcodec.a

# Delegate to build-ffmpeg.sh; it skips already-built libs and reuses the
# cloned source trees under the prefix.
$(FFMPEG_PREFIX)/lib/libavcodec.a:
	$(FFMPEG_SCRIPT) $(FFMPEG_PREFIX) $(JOBS)

## Remove the FFmpeg build for the current target.
ffmpeg-clean:
	rm -rf $(FFMPEG_PREFIX)

## Remove the FFmpeg builds for ALL targets (not the cargo target/).
clean:
	rm -rf target/ffmpeg