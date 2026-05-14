# Ghostty Surface Capture

This helper captures a real GhosttyKit macOS renderer surface to PNG. It is used
for tutorial evidence where the `casars` TUI displays Kitty graphics and the
artifact must come from Ghostty's renderer rather than a synthetic VT replay.

## Build

Build Ghostty's `GhosttyKit.xcframework` first, then point the helper at it:

```sh
git clone --depth 1 https://github.com/ghostty-org/ghostty.git /private/tmp/ghostty-source
cd /private/tmp/ghostty-source

# If this fails with a missing Metal toolchain error, install the Xcode Metal
# component first:
#   xcodebuild -downloadComponent MetalToolchain
#
# This assumes Zig 0.15.2 is on PATH. If it is unpacked somewhere else, prepend
# that directory to PATH for this command.
ZIG_GLOBAL_CACHE_DIR=/private/tmp/zig-cache-global \
ZIG_LOCAL_CACHE_DIR=/private/tmp/zig-cache-local \
zig build \
  -Dapp-runtime=none \
  -Demit-exe=false \
  -Demit-docs=false \
  -Demit-terminfo=false \
  -Demit-termcap=false \
  -Demit-themes=false \
  -Demit-webdata=false \
  -Demit-xcframework=true \
  -Demit-macos-app=false \
  -Dsentry=false \
  --summary failures
```

That produces:

```text
/private/tmp/ghostty-source/macos/GhosttyKit.xcframework
```

Then build the capture helper:

```sh
GHOSTTYKIT_XCFRAMEWORK=/path/to/GhosttyKit.xcframework \
  tools/ghostty-surface-capture/build.sh /tmp/ghostty-surface-capture
```

The current local spike uses:

```sh
GHOSTTYKIT_XCFRAMEWORK=/private/tmp/ghostty-source/macos/GhosttyKit.xcframework \
  tools/ghostty-surface-capture/build.sh /private/tmp/ghostty-surface-capture
```

The build script sets Swift's clang module cache under `/private/tmp` so it does
not write into the user's normal `~/.cache` during agent-driven runs.

## Capture

```sh
/private/tmp/ghostty-surface-capture \
  --output /tmp/tui.png \
  --cwd /path/to/tutorial.pack \
  --width 2200 \
  --height 1400 \
  --font-size 12 \
  --settle-seconds 12 \
  --input-event 500:r \
  -- /path/to/casars msexplore --preset uv_coverage /path/to/data.ms
```

Use `--input TEXT` for text sent immediately after surface creation, and
`--input-event MS:TEXT` for timed input events. Task tutorial captures typically
use one or more `r` events to run and confirm the task.

When run from Codex, this helper must run outside the command sandbox because
GhosttyKit needs access to AppKit and Metal. The window is created offscreen,
and the PNG is written from the IOSurface/CGImage contents of Ghostty's own
renderer layer.
