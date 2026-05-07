import sys
from pathlib import Path

import click

from .changelog import parse_changelog, read_version_from_cargo_toml
from .commits import (
    filter_commits,
    format_commit_summary,
    get_commits_since_tag,
    get_previous_tag,
    load_filter_config,
)
from .config import ReleaseConfig
from .publisher import (
    create_git_tag,
    find_binaries,
    publish_to_api,
    upload_to_s3,
)


@click.group()
def cli() -> None:
    """HITL Daemon release manager."""


@cli.command()
@click.option("--pre", is_flag=True, help="Mark as prerelease")
@click.option("--artifacts-dir", type=click.Path(exists=True), help="Directory containing built binaries")
@click.option("--dry-run", is_flag=True, help="Show what would be done without publishing")
def publish(pre: bool, artifacts_dir: str | None, dry_run: bool) -> None:
    """Publish a new HITL daemon release."""
    config = ReleaseConfig.from_env(artifacts_dir=artifacts_dir)
    daemon_dir = config.daemon_dir

    cargo_path = Path(daemon_dir) / "Cargo.toml"
    if not cargo_path.exists():
        click.echo(f"Error: Cargo.toml not found at {cargo_path}", err=True)
        sys.exit(1)

    version = read_version_from_cargo_toml(cargo_path.read_text())
    click.echo(f"Version: {version}")

    changelog_path = Path(daemon_dir) / "CHANGELOG.md"
    if not changelog_path.exists():
        click.echo(f"Error: CHANGELOG.md not found at {changelog_path}", err=True)
        sys.exit(1)

    release_notes = parse_changelog(changelog_path.read_text(), version)
    if not release_notes:
        click.echo(f"Error: No changelog entry found for version {version}", err=True)
        sys.exit(1)

    click.echo(f"Release notes:\n{release_notes}\n")

    prev_tag = get_previous_tag(daemon_dir)
    commit_summary = ""
    if prev_tag:
        filter_path = Path(daemon_dir) / ".release-filter.yaml"
        filter_config = load_filter_config(str(filter_path))
        raw_commits = get_commits_since_tag(prev_tag, daemon_dir)
        filtered = filter_commits(raw_commits, filter_config, daemon_dir)
        commit_summary = format_commit_summary(filtered)
        if commit_summary:
            click.echo(f"Commit summary ({len(filtered)} commits):\n{commit_summary}\n")
    else:
        click.echo("No previous tag found — skipping commit summary.\n")

    binaries = find_binaries(config, version)
    if not binaries:
        click.echo("Error: No binaries found. Build first or pass --artifacts-dir.", err=True)
        sys.exit(1)

    click.echo(f"Binaries found: {', '.join(binaries.keys())}")

    if dry_run:
        click.echo("\n[DRY RUN] Would upload and publish. Exiting.")
        return

    click.echo("\nUploading to S3...")
    assets = upload_to_s3(config, version, binaries)
    for platform, info in assets.items():
        click.echo(f"  {platform}: {info['s3_key']} ({info['file_size_bytes']} bytes)")

    click.echo("\nPublishing to API...")
    publish_to_api(config, version, release_notes, commit_summary, pre, assets)
    click.echo("Published successfully.")

    tag_created = create_git_tag(version, daemon_dir)
    if tag_created:
        click.echo(f"Created git tag: hitl-daemon-v{version}")
    else:
        click.echo(f"Git tag hitl-daemon-v{version} already exists.")

    click.echo(f"\nDone! Release {version} is live.")


@cli.command(name="list")
def list_releases() -> None:
    """List published releases."""
    config = ReleaseConfig.from_env()

    with httpx.Client(base_url=config.api_url, timeout=10) as client:
        resp = client.get("/api/hitl/releases")
        resp.raise_for_status()
        data = resp.json()

    releases = data.get("releases", [])
    if not releases:
        click.echo("No releases published yet.")
        return

    for r in releases:
        pre_tag = " [pre-release]" if r["is_prerelease"] else ""
        platforms = ", ".join(a["platform"] for a in r.get("assets", []))
        click.echo(f"  v{r['version']}{pre_tag} — {r['published_at'][:10]} — [{platforms}]")


if __name__ == "__main__":
    cli()
