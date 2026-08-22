# HITL Daemon Release Runbook

## Publishing

```bash
cd hitl-daemon
cargo build --release                                          # macOS arm64
cargo build --release --target x86_64-unknown-linux-musl       # Linux
cargo build --release --target x86_64-pc-windows-gnu           # Windows

mkdir -p dist
cp target/release/hitl-daemon dist/hitl-daemon-{version}-macos-arm64
cp target/x86_64-unknown-linux-musl/release/hitl-daemon dist/hitl-daemon-{version}-ubuntu-x86_64
cp target/x86_64-pc-windows-gnu/release/hitl-daemon.exe dist/hitl-daemon-{version}-windows-x86_64.exe

cd ../th3seus-tools && source .venv/bin/activate
HITL_RELEASE_TOKEN="$(aws ssm get-parameter --name /th3seus/prod/hitl-release-token --with-decryption --query Parameter.Value --output text)" \
HITL_DAEMON_DIR="../hitl-daemon" \
HITL_MIN_SUPPORTED_VERSION="0.11.0" \
  python -m release_daemon publish --artifacts-dir ../hitl-daemon/dist
```

Pre-publish: bump `Cargo.toml`, update `CHANGELOG.md`.

`HITL_MIN_SUPPORTED_VERSION` (or `--min-supported-version`) is **required**. The
API rejects a release without it, because clients use it to distinguish
"outdated but usable" from "too old to talk to this web app". Set it to the
oldest daemon version the currently deployed web app can still work with — not
to the version being published.

## Code signing: what we do, and what we deliberately do not

**What happens today.** `release_daemon` ad-hoc signs macOS artifacts
(`codesign --sign -`) before upload, and refuses to publish a macOS artifact
that carries no signature at all. Linux and Windows artifacts are published
unsigned.

**Why ad-hoc is enough for the primary path.** Apple Silicon refuses to execute
an arm64 binary with no signature whatsoever; an ad-hoc signature satisfies that
and costs nothing. The Gatekeeper and SmartScreen prompts that a *paid*
certificate would remove are not triggered by our install path at all: macOS
attaches `com.apple.quarantine` only when a quarantine-aware application does
the download (browsers, Mail — not `curl`), and Windows SmartScreen keys off
Mark-of-the-Web, which browsers write and `Invoke-WebRequest` does not. Since
users install by copying a command that runs `curl`, neither mark is present.

**What this costs users.** Anyone who bypasses the install command and downloads
an artifact directly from the releases page in a browser *will* hit Gatekeeper
or SmartScreen. That path is not silently broken — the releases page states the
trust steps and gives the exact commands — but it is worse than it would be with
real certificates.

**What protects update integrity in the meantime.** An ad-hoc signature
authenticates nothing about the publisher. Integrity rests on the SHA-256 the
release channel publishes, fetched over HTTPS, and checked by the install script
and by daemon self-update before anything is swapped into place. This is weaker
than a certificate chain: it assumes the release channel itself is not
compromised, and it does not survive an artifact being mirrored anywhere outside
our own S3 bucket.

**When to buy certificates.** Two triggers:

1. Browser downloads from the releases page start mattering enough to justify
   the recurring cost — roughly $99/yr for an Apple Developer Program
   membership, plus a Windows certificate (Azure Trusted Signing is the cheapest
   route; OV and EV certificates from a CA cost more and require the private key
   on a hardware token or cloud HSM).
2. Artifacts get mirrored, distributed by a third party, or served from anywhere
   other than our own release host. At that point the published-hash-over-HTTPS
   assumption no longer holds and signature provenance becomes load-bearing.

**What to change when that happens.** Replace `adhoc_sign` in
`release_daemon/signing.py` with real signing (`codesign --sign "Developer ID
Application: ..."` plus `notarytool submit --wait` on macOS; `signtool sign` on
Windows), and extend `_REQUIRES_SIGNATURE` to include the platforms now covered.
`verify_signed` already fails the publish when a required signature is missing,
so the guard does not need to change. Nothing else in the release flow depends
on which kind of signature is applied.

## Install scripts

`release_daemon/scripts/install.sh` and `install.ps1` are uploaded as
`install_script` release assets on every publish. `install.sh` is registered for
both macOS and Linux against a single uploaded object — the script detects the
host itself, so duplicating the bytes per platform would only let the two copies
drift.

Users obtain them by signing in to the website and copying the command, which
carries presigned URLs for both the script and the platform binary plus the
binary's expected hash. The install script runs on a machine with no session and
therefore cannot authenticate for itself; this is why the URLs are passed in
rather than looked up.

Presigned links expire (1 hour). Both scripts detect an expired link
specifically — S3 answers with HTTP 403 and `Request has expired` — and say so,
rather than surfacing it as a permissions or transport error.

**Windows caveat:** `install.ps1` has never been executed. It was written and
accepted on inspection; verification on a real Windows host is outstanding.

## Multi-environment

```bash
# Staging
RELEASE_API_URL=https://playground-api.th3seus.net \
RELEASE_S3_BUCKET=th3seus-artifacts-playground \
HITL_MIN_SUPPORTED_VERSION="0.11.0" \
HITL_RELEASE_TOKEN="$(aws ssm get-parameter --name /th3seus/playground/hitl-release-token --with-decryption --query Parameter.Value --output text)" \
  python -m release_daemon publish --artifacts-dir ../hitl-daemon/dist
```
