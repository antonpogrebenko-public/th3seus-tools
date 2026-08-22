"""Artifact signing at publish time.

Paid certificates are deferred, so this is ad-hoc signing only: enough to let
Apple Silicon execute an arm64 binary, not enough to authenticate a publisher.
Platforms with no free signing mechanism are published unsigned and must never
be represented otherwise.
"""

from pathlib import Path

import pytest

from release_daemon.signing import (
    SigningError,
    adhoc_sign,
    platform_requires_signature,
    verify_signed,
)


@pytest.fixture()
def fake_binary(tmp_path: Path) -> Path:
    path = tmp_path / "hitl-daemon-9.9.9-macos-arm64"
    path.write_bytes(b"\xcf\xfa\xed\xfe" + b"\x00" * 256)
    path.chmod(0o755)
    return path


def test_only_macos_requires_a_signature():
    # Apple Silicon refuses to execute an unsigned arm64 binary; ELF and PE
    # have no free equivalent, so requiring one there would be unmeetable.
    assert platform_requires_signature("macos") is True
    assert platform_requires_signature("ubuntu") is False
    assert platform_requires_signature("windows") is False


def test_verify_signed_rejects_unsigned_macos_artifact(fake_binary):
    with pytest.raises(SigningError) as exc:
        verify_signed("macos", fake_binary)
    assert "signature" in str(exc.value).lower()


def test_verify_signed_passes_through_platforms_without_signing(tmp_path):
    path = tmp_path / "hitl-daemon-9.9.9-ubuntu-x86_64"
    path.write_bytes(b"\x7fELF" + b"\x00" * 256)
    # Must not raise: unsigned is the correct, honest state for this platform.
    verify_signed("ubuntu", path)


@pytest.mark.skipif(
    not Path("/usr/bin/codesign").exists(), reason="codesign only exists on macOS"
)
def test_adhoc_sign_then_verify_roundtrip(tmp_path):
    """A real Mach-O binary, ad-hoc signed, verifies."""
    import shutil

    real = tmp_path / "hitl-daemon-9.9.9-macos-arm64"
    shutil.copy("/bin/echo", real)
    adhoc_sign("macos", real)
    verify_signed("macos", real)
