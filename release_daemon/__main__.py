import sys
from pathlib import Path

import click
import httpx

from .changelog import parse_changelog, read_version_from_cargo_toml
from .commits import (
    filter_commits,
    format_commit_summary,
    get_commits_since_tag,
    get_previous_tag,
    load_filter_config,
)
from .config import ReleaseConfig
from .drift import check_drift, format_report, has_drift
from .publisher import (
    create_git_tag,
    find_binaries,
    publish_to_api,
    upload_install_scripts,
    upload_to_s3,
)


@click.group()
def cli() -> None:
    """HITL Daemon release manager."""


@cli.command()
@click.option("--pre", is_flag=True, help="Mark as prerelease")
@click.option("--artifacts-dir", type=click.Path(exists=True), help="Directory containing built binaries")
@click.option("--dry-run", is_flag=True, help="Show what would be done without publishing")
@click.option(
    "--min-supported-version",
    envvar="HITL_MIN_SUPPORTED_VERSION",
    help=(
        "Lowest daemon version current web clients can talk to. Required: the "
        "API rejects a release whose compatibility floor is unknown."
    ),
)
def publish(
    pre: bool, artifacts_dir: str | None, dry_run: bool, min_supported_version: str | None
) -> None:
    """Publish a new HITL daemon release."""
    try:
        config = ReleaseConfig.from_env(artifacts_dir=artifacts_dir)
    except ValueError as exc:
        # A missing credential is a thing to state, not a stack trace to read.
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)
    daemon_dir = config.daemon_dir

    cargo_path = Path(daemon_dir) / "Cargo.toml"
    if not cargo_path.exists():
        click.echo(f"Error: Cargo.toml not found at {cargo_path}", err=True)
        sys.exit(1)

    version = read_version_from_cargo_toml(cargo_path.read_text())
    click.echo(f"Version: {version}")

    if not min_supported_version:
        click.echo(
            "Error: --min-supported-version (or HITL_MIN_SUPPORTED_VERSION) is "
            "required. Clients use it to tell 'outdated but usable' from 'too "
            "old to talk to this web app'.",
            err=True,
        )
        sys.exit(1)
    click.echo(f"Minimum supported daemon version: {min_supported_version}")

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
    assets.extend(upload_install_scripts(config, version))
    for info in assets:
        click.echo(
            f"  {info['platform']}/{info['kind']}: {info['s3_key']} "
            f"({info['file_size_bytes']} bytes)"
        )

    click.echo("\nPublishing to API...")
    publish_to_api(
        config, version, release_notes, commit_summary, pre, assets, min_supported_version
    )
    click.echo("Published successfully.")

    tag_created = create_git_tag(version, daemon_dir)
    if tag_created:
        click.echo(f"Created git tag: hitl-daemon-v{version}")
    else:
        click.echo(f"Git tag hitl-daemon-v{version} already exists.")

    click.echo(f"\nDone! Release {version} is live.")


@cli.command(name="list")
def list_releases() -> None:
    """List published releases.

    Reads the channel and nothing more, so it asks for no publishing token.
    """
    with httpx.Client(base_url=ReleaseConfig.api_url_from_env(), timeout=10) as client:
        resp = client.get("/api/hitl/releases")
        resp.raise_for_status()
        data = resp.json()

    releases = data.get("releases", [])
    if not releases:
        click.echo("No releases published yet.")
        return

    for r in releases:
        pre_tag = " [pre-release]" if r["is_prerelease"] else ""
        # One entry per platform. A release carries an install script as well as
        # a binary for each, and listing both printed every platform twice.
        # Assets published before the kind column existed are all binaries.
        seen: list[str] = []
        for a in r.get("assets", []):
            if a.get("kind", "binary") != "binary" or a["platform"] in seen:
                continue
            seen.append(a["platform"])
        click.echo(
            f"  v{r['version']}{pre_tag} — {r['published_at'][:10]} — [{', '.join(seen)}]"
        )


@cli.command("check-drift")
@click.option(
    "--daemon-dir",
    envvar="HITL_DAEMON_DIR",
    default=None,
    help="Path to the hitl-daemon source tree (default: ../hitl-daemon).",
)
def check_drift_command(daemon_dir: str | None) -> None:
    """Report which environments serve a different daemon than this tree builds.

    Read-only, and it publishes nothing. Publishing stays a separate, explicit
    act per environment; this exists so the gap is visible before a user finds
    it on the releases page.

    Exits non-zero when any environment is out of step or could not be read, so
    it is usable as a check rather than only as a thing to read.
    """
    daemon_path = Path(daemon_dir) if daemon_dir else Path.cwd().parent / "hitl-daemon"
    cargo_path = daemon_path / "Cargo.toml"
    if not cargo_path.exists():
        click.echo(f"Error: Cargo.toml not found at {cargo_path}", err=True)
        sys.exit(1)

    local_version = read_version_from_cargo_toml(cargo_path.read_text())
    click.echo(f"Local tree builds: {local_version}\n")

    reports = check_drift(local_version)
    for report in reports:
        click.echo(f"  {format_report(report)}")

    if has_drift(reports):
        click.echo("\nOne or more environments are out of step.")
        sys.exit(1)
    click.echo("\nEvery environment is in sync.")


if __name__ == "__main__":
    cli()
