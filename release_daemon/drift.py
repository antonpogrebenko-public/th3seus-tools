"""Which environments are serving an older daemon than the source tree builds.

Playground sat seven versions behind the working tree for weeks, and the way it
surfaced was a user opening the releases page and noticing. Publishing is
deliberately a separate, explicit act per environment — that is not the problem.
Not being able to see the gap without visiting a web page is.

Read-only. This reports; it never publishes.
"""

import json
from dataclasses import dataclass
from typing import Protocol
from urllib.request import urlopen


@dataclass(frozen=True)
class Environment:
    """A release channel to compare the source tree against."""

    name: str
    api_url: str


# Production is listed so the gap is visible, not so it gets closed
# automatically. Publishing there stays an explicit decision.
DEFAULT_ENVIRONMENTS: tuple[Environment, ...] = (
    Environment(name="playground", api_url="https://playground-api.th3seus.net"),
    Environment(name="production", api_url="https://api.th3seus.net"),
)


@dataclass(frozen=True)
class DriftReport:
    """What one environment is serving, against what the tree builds."""

    environment: str
    local_version: str
    published_version: str | None
    """None when the channel could not be read or has published nothing."""
    error: str | None = None

    @property
    def is_behind(self) -> bool:
        """Whether the channel serves something other than the local version.

        Deliberately an inequality rather than a version comparison. An
        environment ahead of the working tree is just as much a surprise as one
        behind it, and silently accepting it would hide a bad publish.
        """
        if self.published_version is None:
            return False
        return self.published_version != self.local_version

    @property
    def is_unreadable(self) -> bool:
        return self.error is not None


class VersionFetcher(Protocol):
    def __call__(self, api_url: str) -> str | None: ...


def fetch_latest_version(api_url: str) -> str | None:
    """The newest stable version an environment serves, or None if it has none."""
    with urlopen(f"{api_url}/api/hitl/releases/latest", timeout=15) as response:
        payload = json.load(response)
    version = payload.get("version")
    return str(version) if version else None


def check_drift(
    local_version: str,
    environments: tuple[Environment, ...] = DEFAULT_ENVIRONMENTS,
    fetcher: VersionFetcher = fetch_latest_version,
) -> list[DriftReport]:
    """Compare every environment against the version the source tree builds.

    An unreachable channel is reported as unreadable rather than as in sync: a
    network failure is not evidence that anything matches.
    """
    reports: list[DriftReport] = []
    for environment in environments:
        try:
            published = fetcher(environment.api_url)
        except Exception as exc:  # noqa: BLE001 - reported per environment
            reports.append(
                DriftReport(
                    environment=environment.name,
                    local_version=local_version,
                    published_version=None,
                    error=str(exc),
                )
            )
            continue
        reports.append(
            DriftReport(
                environment=environment.name,
                local_version=local_version,
                published_version=published,
            )
        )
    return reports


def format_report(report: DriftReport) -> str:
    """One line per environment, saying what to do about it."""
    if report.is_unreadable:
        return (
            f"{report.environment}: could not read the release channel ({report.error})"
        )
    if report.published_version is None:
        return (
            f"{report.environment}: no release published; "
            f"local tree builds {report.local_version}"
        )
    if report.is_behind:
        return (
            f"{report.environment}: serving {report.published_version}, "
            f"local tree builds {report.local_version}"
        )
    return f"{report.environment}: in sync at {report.local_version}"


def has_drift(reports: list[DriftReport]) -> bool:
    """Whether anything needs attention, including a channel that would not answer."""
    return any(r.is_behind or r.is_unreadable for r in reports)
