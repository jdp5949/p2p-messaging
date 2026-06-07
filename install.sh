#!/bin/sh
# p2p installer: detects OS/arch, downloads the matching release binary, and
# installs it onto PATH. Usage:
#   curl -fsSL https://raw.githubusercontent.com/jdp5949/p2p-messaging/main/install.sh | sh
set -eu

REPO="jdp5949/p2p-messaging"

# map_os_arch echoes "<os> <arch>" or exits non-zero with a message.
map_os_arch() {
	os=$(uname -s 2>/dev/null || echo unknown)
	arch=$(uname -m 2>/dev/null || echo unknown)
	case "$os" in
		Darwin) os=darwin ;;
		Linux)  os=linux ;;
		*) echo "unsupported OS: $os (see https://github.com/$REPO/releases)" >&2; return 1 ;;
	esac
	case "$arch" in
		x86_64|amd64) arch=amd64 ;;
		aarch64|arm64) arch=arm64 ;;
		*) echo "unsupported arch: $arch (see https://github.com/$REPO/releases)" >&2; return 1 ;;
	esac
	echo "$os $arch"
}

# Allow a self-test: `OS_ARCH_SELFTEST=1 sh install.sh` just prints mapping.
if [ "${OS_ARCH_SELFTEST:-}" = "1" ]; then
	map_os_arch
	exit $?
fi

set -- $(map_os_arch)
OS=$1
ARCH=$2
ASSET="p2p-${OS}-${ARCH}"
URL="https://github.com/${REPO}/releases/latest/download/${ASSET}"

TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT

echo "Downloading ${ASSET}…" >&2
if command -v curl >/dev/null 2>&1; then
	curl -fsSL "$URL" -o "$TMP"
elif command -v wget >/dev/null 2>&1; then
	wget -qO "$TMP" "$URL"
else
	echo "need curl or wget" >&2
	exit 1
fi
chmod +x "$TMP"

# macOS: clear quarantine so Gatekeeper does not block.
if [ "$OS" = "darwin" ]; then
	xattr -d com.apple.quarantine "$TMP" 2>/dev/null || true
fi

DEST="/usr/local/bin/p2p"
install_to() { mv "$TMP" "$1" && chmod +x "$1"; }

if [ -w "$(dirname "$DEST")" ] || [ "$(id -u)" = "0" ]; then
	install_to "$DEST"
elif command -v sudo >/dev/null 2>&1 && [ -t 0 ]; then
	sudo mv "$TMP" "$DEST" && sudo chmod +x "$DEST"
else
	DEST="$HOME/.local/bin/p2p"
	mkdir -p "$(dirname "$DEST")"
	install_to "$DEST"
	case ":$PATH:" in
		*":$HOME/.local/bin:"*) ;;
		*) echo "note: add $HOME/.local/bin to your PATH" >&2 ;;
	esac
fi
trap - EXIT

echo "✓ installed p2p to $DEST" >&2
echo "run: p2p send" >&2
