import hashlib
import subprocess
from pathlib import Path

import boto3
import httpx

from .config import ReleaseConfig
from .signing import adhoc_sign, verify_signed

PLATFORM_SUFFIXES = {
    "macos": "macos-arm64",
    "ubuntu": "ubuntu-x86_64",
    "windows": "windows-x86_64.exe",
}


def compute_sha256(filepath: Path) -> str:
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def find_binaries(config: ReleaseConfig, version: str) -> dict[str, Path]:
    search_dir = Path(config.artifacts_dir) if config.artifacts_dir else Path(config.daemon_dir) / "target" / "release"

    found = {}
    for platform, suffix in PLATFORM_SUFFIXES.items():
        filename = f"hitl-daemon-{version}-{suffix}"
        path = search_dir / filename
        if path.exists():
            found[platform] = path

    return found


SCRIPTS_DIR = Path(__file__).parent / "scripts"

# install.sh serves both POSIX platforms; install.ps1 serves Windows.
INSTALL_SCRIPTS = {
    "install.sh": ("macos", "ubuntu"),
    "install.ps1": ("windows",),
}


def upload_install_scripts(config: ReleaseConfig, version: str) -> list[dict]:
    """Upload the install scripts and describe them as release assets.

    install.sh is registered for both macOS and Linux against a single uploaded
    object: the script detects the host itself, so duplicating the bytes per
    platform would only create a way for the two copies to drift.
    """
    s3 = boto3.client("s3")
    assets: list[dict] = []

    for filename, platforms in INSTALL_SCRIPTS.items():
        path = SCRIPTS_DIR / filename
        if not path.exists():
            raise FileNotFoundError(f"Install script missing: {path}")

        s3_key = f"hitl-releases/{version}/{filename}"
        s3.upload_file(str(path), config.s3_bucket, s3_key)
        digest = compute_sha256(path)
        size = path.stat().st_size

        for platform in platforms:
            assets.append(
                {
                    "platform": platform,
                    "kind": "install_script",
                    "s3_key": s3_key,
                    "filename": filename,
                    "file_size_bytes": size,
                    "sha256": digest,
                }
            )

    return assets


def upload_to_s3(config: ReleaseConfig, version: str, binaries: dict[str, Path]) -> list[dict]:
    """Sign, verify, then upload.

    Signing happens before the hash is computed and before anything reaches the
    bucket: `codesign` rewrites the binary, so hashing first would publish a
    digest that no longer matches the artifact users download. Verification
    failing aborts the whole publish rather than uploading an artifact Apple
    Silicon would refuse to execute.
    """
    s3 = boto3.client("s3")
    results: list[dict] = []

    for platform, path in binaries.items():
        adhoc_sign(platform, path)
        verify_signed(platform, path)

    for platform, path in binaries.items():
        s3_key = f"hitl-releases/{version}/{path.name}"
        s3.upload_file(str(path), config.s3_bucket, s3_key)
        results.append(
            {
                "platform": platform,
                "kind": "binary",
                "s3_key": s3_key,
                "filename": path.name,
                # Hashed after signing: codesign rewrites the binary, so a
                # digest taken earlier would not match what users download.
                "file_size_bytes": path.stat().st_size,
                "sha256": compute_sha256(path),
            }
        )

    return results


def publish_to_api(
    config: ReleaseConfig,
    version: str,
    release_notes: str,
    commit_summary: str,
    is_prerelease: bool,
    assets: list[dict],
    min_supported_daemon_version: str,
) -> None:
    headers = {"X-Release-Token": config.release_token, "Content-Type": "application/json"}

    with httpx.Client(base_url=config.api_url, headers=headers, timeout=30) as client:
        resp = client.post(
            "/api/hitl/releases",
            json={
                "version": version,
                "release_notes": release_notes,
                "commit_summary": commit_summary,
                "is_prerelease": is_prerelease,
                "min_supported_daemon_version": min_supported_daemon_version,
            },
        )
        resp.raise_for_status()

        for asset_data in assets:
            resp = client.post(
                f"/api/hitl/releases/{version}/assets",
                json=asset_data,
            )
            resp.raise_for_status()


def create_git_tag(version: str, repo_dir: str) -> bool:
    tag = f"hitl-daemon-v{version}"
    result = subprocess.run(
        ["git", "tag", "-l", tag],
        capture_output=True,
        text=True,
        cwd=repo_dir,
    )
    if tag in result.stdout:
        return False

    subprocess.run(
        ["git", "tag", "-a", tag, "-m", f"HITL daemon release {version}"],
        check=True,
        cwd=repo_dir,
    )
    return True
