#!/bin/sh
# HITL daemon installer.
#
# Obtained by signing in to the website and copying the command, which carries a
# presigned artifact URL and its expected hash. Both expire, so this script
# treats an expired link as a first-class outcome rather than a transport error.
#
#   curl -fsSL "<script-url>" | sh -s -- --url "<artifact-url>" --sha256 "<hash>"

set -eu

ARTIFACT_URL=""
EXPECTED_SHA=""
INSTALL_DIR="${HITL_INSTALL_DIR:-$HOME/.local/bin}"

EXIT_USAGE=2
EXIT_UNSUPPORTED=3
EXIT_EXPIRED=4
EXIT_CHECKSUM=5
EXIT_DOWNLOAD=6

die() {
    code=$1
    shift
    printf 'error: %s\n' "$*" >&2
    exit "$code"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --url) ARTIFACT_URL="${2:-}"; shift 2 ;;
        --sha256) EXPECTED_SHA="${2:-}"; shift 2 ;;
        --install-dir) INSTALL_DIR="${2:-}"; shift 2 ;;
        -h|--help)
            printf 'usage: install.sh --url <artifact-url> --sha256 <hash> [--install-dir <dir>]\n'
            exit 0 ;;
        *) die $EXIT_USAGE "unknown argument: $1" ;;
    esac
done

[ -n "$ARTIFACT_URL" ] || die $EXIT_USAGE "--url is required. Copy the install command from the website."
[ -n "$EXPECTED_SHA" ] || die $EXIT_USAGE "--sha256 is required. Copy the install command from the website."

# Platform
os=$(uname -s)
arch=$(uname -m)
case "$os/$arch" in
    Darwin/arm64) ;;
    Linux/x86_64) ;;
    *)
        die $EXIT_UNSUPPORTED "no daemon is published for $os/$arch. Supported: Darwin/arm64, Linux/x86_64." ;;
esac

# Download. Nothing is written outside this temp directory until every check has
# passed, so a failure anywhere leaves no partial installation behind.
tmp=$(mktemp -d)
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

printf 'Downloading daemon...\n'
http_code=$(curl -sS -L -w '%{http_code}' -o "$tmp/hitl-daemon" "$ARTIFACT_URL" || echo "000")

if [ "$http_code" = "403" ] || [ "$http_code" = "400" ]; then
    # S3 answers an expired presigned URL with 403 and "Request has expired" in
    # the body. Surfacing that as-is would read as a permissions problem.
    if grep -qi 'Request has expired\|ExpiredToken' "$tmp/hitl-daemon" 2>/dev/null; then
        die $EXIT_EXPIRED "this install link has expired. Sign in to the website and copy a fresh command."
    fi
    die $EXIT_DOWNLOAD "download refused (HTTP $http_code). The link may have expired or been revoked."
fi

[ "$http_code" = "200" ] || die $EXIT_DOWNLOAD "download failed (HTTP $http_code)."

# Verify
if command -v sha256sum >/dev/null 2>&1; then
    actual=$(sha256sum "$tmp/hitl-daemon" | cut -d' ' -f1)
elif command -v shasum >/dev/null 2>&1; then
    actual=$(shasum -a 256 "$tmp/hitl-daemon" | cut -d' ' -f1)
else
    die $EXIT_CHECKSUM "neither sha256sum nor shasum is available to verify the download."
fi

if [ "$actual" != "$EXPECTED_SHA" ]; then
    die $EXIT_CHECKSUM "checksum mismatch. Expected $EXPECTED_SHA, got $actual. The download was not installed."
fi

# Install
mkdir -p "$INSTALL_DIR" || die $EXIT_DOWNLOAD "cannot create $INSTALL_DIR"
chmod +x "$tmp/hitl-daemon"
# mv over an existing daemon replaces it; re-running the installer is an upgrade.
mv -f "$tmp/hitl-daemon" "$INSTALL_DIR/hitl-daemon" ||
    die $EXIT_DOWNLOAD "cannot write to $INSTALL_DIR. Set HITL_INSTALL_DIR to a writable directory."

installed_version=$("$INSTALL_DIR/hitl-daemon" --version 2>/dev/null || echo "unknown")
printf 'Installed %s to %s\n' "$installed_version" "$INSTALL_DIR/hitl-daemon"

case ":$PATH:" in
    *":$INSTALL_DIR:"*) ;;
    *) printf '\nNote: %s is not on your PATH. Add it, or run the daemon by full path.\n' "$INSTALL_DIR" ;;
esac

printf '\nRun it with:\n  %s/hitl-daemon\n' "$INSTALL_DIR"
