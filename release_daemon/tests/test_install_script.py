"""Behaviour of the POSIX install script.

Driven against a local HTTP server rather than S3, so the expired-link path is
exercised with the exact response body S3 returns.
"""

import hashlib
import http.server
import subprocess
import threading
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "install.sh"

EXIT_USAGE = 2
EXIT_UNSUPPORTED = 3
EXIT_EXPIRED = 4
EXIT_CHECKSUM = 5

# What S3 actually returns once a presigned URL's expiry has passed.
S3_EXPIRED_BODY = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n'
    b"<Error><Code>AccessDenied</Code>"
    b"<Message>Request has expired</Message>"
    b"<Expires>2026-08-22T00:00:00Z</Expires></Error>"
)

# A tiny executable stand-in for the daemon, so --version has something to print.
FAKE_DAEMON = b"#!/bin/sh\necho 'hitl-daemon 9.9.9'\n"
FAKE_DAEMON_SHA = hashlib.sha256(FAKE_DAEMON).hexdigest()


class _Handler(http.server.BaseHTTPRequestHandler):
    mode = "ok"

    def do_GET(self):  # noqa: N802 - stdlib naming
        if self.mode == "expired":
            self.send_response(403)
            self.send_header("Content-Length", str(len(S3_EXPIRED_BODY)))
            self.end_headers()
            self.wfile.write(S3_EXPIRED_BODY)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(FAKE_DAEMON)))
        self.end_headers()
        self.wfile.write(FAKE_DAEMON)

    def log_message(self, *a):
        return


@pytest.fixture()
def server():
    servers = []

    def factory(mode: str = "ok") -> str:
        handler = type("H", (_Handler,), {"mode": mode})
        httpd = http.server.HTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        servers.append(httpd)
        return f"http://127.0.0.1:{httpd.server_port}/artifact"

    yield factory
    for httpd in servers:
        httpd.shutdown()


def run_script(args, home: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["sh", str(SCRIPT), *args],
        capture_output=True,
        text=True,
        env={"HOME": str(home), "PATH": "/usr/bin:/bin:/usr/sbin:/sbin"},
    )


def test_script_is_valid_posix_shell():
    result = subprocess.run(["sh", "-n", str(SCRIPT)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_installs_and_reports_version(server, tmp_path):
    url = server("ok")
    target = tmp_path / "bin"
    result = run_script(
        ["--url", url, "--sha256", FAKE_DAEMON_SHA, "--install-dir", str(target)], tmp_path
    )
    assert result.returncode == 0, result.stderr
    installed = target / "hitl-daemon"
    assert installed.exists()
    assert installed.stat().st_mode & 0o111, "installed daemon must be executable"
    assert "9.9.9" in result.stdout


def test_reinstall_replaces_existing_daemon(server, tmp_path):
    url = server("ok")
    target = tmp_path / "bin"
    target.mkdir()
    stale = target / "hitl-daemon"
    stale.write_bytes(b"#!/bin/sh\necho 'hitl-daemon 0.0.1'\n")
    stale.chmod(0o755)

    result = run_script(
        ["--url", url, "--sha256", FAKE_DAEMON_SHA, "--install-dir", str(target)], tmp_path
    )
    assert result.returncode == 0, result.stderr
    assert "9.9.9" in result.stdout


def test_expired_link_reports_expiry_not_transport_error(server, tmp_path):
    url = server("expired")
    target = tmp_path / "bin"
    result = run_script(
        ["--url", url, "--sha256", FAKE_DAEMON_SHA, "--install-dir", str(target)], tmp_path
    )
    assert result.returncode == EXIT_EXPIRED
    assert "expired" in result.stderr.lower()
    assert not (target / "hitl-daemon").exists(), "no partial installation"


def test_checksum_mismatch_installs_nothing(server, tmp_path):
    url = server("ok")
    target = tmp_path / "bin"
    result = run_script(
        ["--url", url, "--sha256", "0" * 64, "--install-dir", str(target)], tmp_path
    )
    assert result.returncode == EXIT_CHECKSUM
    assert "checksum" in result.stderr.lower()
    assert not (target / "hitl-daemon").exists()


def test_missing_arguments_are_rejected(tmp_path):
    result = run_script(["--url", "https://example.invalid/x"], tmp_path)
    assert result.returncode == EXIT_USAGE
    assert "sha256" in result.stderr.lower()
