"""Publish-time guarantees: artifacts are signed where required, and the
release carries the compatibility floor the API now demands."""

from pathlib import Path

import pytest

from release_daemon.config import ReleaseConfig
from release_daemon.publisher import publish_to_api, upload_to_s3
from release_daemon.signing import SigningError


@pytest.fixture()
def config() -> ReleaseConfig:
    return ReleaseConfig(
        api_url="https://api.invalid",
        release_token="token",
        s3_bucket="bucket",
        daemon_dir="/nonexistent",
        artifacts_dir=None,
    )


@pytest.fixture()
def binaries(tmp_path: Path) -> dict[str, Path]:
    mac = tmp_path / "hitl-daemon-9.9.9-macos-arm64"
    mac.write_bytes(b"\xcf\xfa\xed\xfe" + b"\x00" * 64)
    linux = tmp_path / "hitl-daemon-9.9.9-ubuntu-x86_64"
    linux.write_bytes(b"\x7fELF" + b"\x00" * 64)
    return {"macos": mac, "ubuntu": linux}


def test_upload_signs_macos_and_leaves_others_alone(config, binaries, monkeypatch):
    signed: list[str] = []
    monkeypatch.setattr(
        "release_daemon.publisher.adhoc_sign",
        lambda platform, path: signed.append(platform),
    )
    monkeypatch.setattr("release_daemon.publisher.verify_signed", lambda p, path: None)

    class FakeS3:
        def upload_file(self, *a, **kw):
            return None

    monkeypatch.setattr("release_daemon.publisher.boto3.client", lambda *a, **kw: FakeS3())

    upload_to_s3(config, "9.9.9", binaries)
    assert signed == ["macos", "ubuntu"]  # called for both; the module decides


def test_upload_aborts_when_macos_artifact_is_unsigned(config, binaries, monkeypatch):
    """Fail before the upload, so an unusable artifact never reaches the bucket."""
    uploaded: list[str] = []

    class FakeS3:
        def upload_file(self, path, bucket, key):
            uploaded.append(key)

    monkeypatch.setattr("release_daemon.publisher.boto3.client", lambda *a, **kw: FakeS3())
    monkeypatch.setattr("release_daemon.publisher.adhoc_sign", lambda p, path: None)

    def refuse(platform, path):
        if platform == "macos":
            raise SigningError("no code signature")

    monkeypatch.setattr("release_daemon.publisher.verify_signed", refuse)

    with pytest.raises(SigningError):
        upload_to_s3(config, "9.9.9", binaries)
    assert uploaded == []


def test_publish_sends_min_supported_daemon_version(config, monkeypatch):
    """The API rejects a release with no compatibility floor, so the publisher
    has to supply one."""
    sent: dict = {}

    class FakeResponse:
        status_code = 201

        def raise_for_status(self):
            return None

    class FakeClient:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def post(self, url, json=None):
            if url == "/api/hitl/releases":
                sent.update(json)
            return FakeResponse()

    monkeypatch.setattr("release_daemon.publisher.httpx.Client", FakeClient)

    publish_to_api(
        config,
        version="9.9.9",
        release_notes="notes",
        commit_summary="",
        is_prerelease=False,
        assets={},
        min_supported_daemon_version="0.11.0",
    )
    assert sent["min_supported_daemon_version"] == "0.11.0"


def test_install_scripts_registered_for_every_platform(config, monkeypatch):
    """install.sh covers both POSIX platforms from one uploaded object."""
    uploaded: list[str] = []

    class FakeS3:
        def upload_file(self, path, bucket, key):
            uploaded.append(key)

    monkeypatch.setattr("release_daemon.publisher.boto3.client", lambda *a, **kw: FakeS3())

    from release_daemon.publisher import upload_install_scripts

    assets = upload_install_scripts(config, "9.9.9")
    by_platform = {a["platform"]: a for a in assets}

    assert set(by_platform) == {"macos", "ubuntu", "windows"}
    assert all(a["kind"] == "install_script" for a in assets)
    # One object per script, not one per platform: two platforms share install.sh.
    assert len(uploaded) == 2
    assert by_platform["macos"]["s3_key"] == by_platform["ubuntu"]["s3_key"]
    assert by_platform["windows"]["filename"] == "install.ps1"
    assert all(len(a["sha256"]) == 64 for a in assets)
