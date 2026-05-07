import hashlib
import subprocess
from pathlib import Path

import boto3
import httpx

from .config import ReleaseConfig

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


def upload_to_s3(config: ReleaseConfig, version: str, binaries: dict[str, Path]) -> dict[str, dict]:
    s3 = boto3.client("s3")
    results = {}

    for platform, path in binaries.items():
        s3_key = f"hitl-releases/{version}/{path.name}"
        s3.upload_file(str(path), config.s3_bucket, s3_key)
        results[platform] = {
            "s3_key": s3_key,
            "filename": path.name,
            "file_size_bytes": path.stat().st_size,
            "sha256": compute_sha256(path),
        }

    return results


def publish_to_api(
    config: ReleaseConfig,
    version: str,
    release_notes: str,
    commit_summary: str,
    is_prerelease: bool,
    assets: dict[str, dict],
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
            },
        )
        resp.raise_for_status()

        for platform, asset_data in assets.items():
            resp = client.post(
                f"/api/hitl/releases/{version}/assets",
                json={"platform": platform, **asset_data},
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
