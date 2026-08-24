"""Drift reporting: what each environment serves versus what the tree builds."""

from release_daemon.drift import (
    Environment,
    check_drift,
    format_report,
    has_drift,
)

ENVIRONMENTS = (
    Environment(name="playground", api_url="https://playground-api.example.invalid"),
    Environment(name="production", api_url="https://api.example.invalid"),
)


def fetcher_returning(versions: dict[str, str | None]):
    def _fetch(api_url: str) -> str | None:
        return versions[api_url]

    return _fetch


def test_reports_an_environment_that_is_behind():
    reports = check_drift(
        "0.18.1",
        ENVIRONMENTS,
        fetcher_returning(
            {
                "https://playground-api.example.invalid": "0.16.1",
                "https://api.example.invalid": "0.18.1",
            }
        ),
    )

    playground, production = reports
    assert playground.is_behind
    assert not production.is_behind
    assert has_drift(reports)


def test_reports_everything_in_sync():
    reports = check_drift(
        "0.18.1",
        ENVIRONMENTS,
        fetcher_returning(
            {
                "https://playground-api.example.invalid": "0.18.1",
                "https://api.example.invalid": "0.18.1",
            }
        ),
    )

    assert not has_drift(reports)
    assert "in sync at 0.18.1" in format_report(reports[0])


def test_an_environment_ahead_of_the_tree_is_also_drift():
    # Just as much a surprise as one behind, and quietly accepting it would
    # hide a publish that should not have happened.
    reports = check_drift(
        "0.18.1",
        ENVIRONMENTS[:1],
        fetcher_returning({"https://playground-api.example.invalid": "0.19.0"}),
    )

    assert reports[0].is_behind
    assert has_drift(reports)


def test_an_unreachable_channel_is_not_reported_as_in_sync():
    # A network failure is not evidence that anything matches.
    def _explode(api_url: str) -> str | None:
        raise OSError("connection refused")

    reports = check_drift("0.18.1", ENVIRONMENTS[:1], _explode)

    assert reports[0].is_unreadable
    assert not reports[0].is_behind
    assert has_drift(reports)
    assert "could not read" in format_report(reports[0])


def test_a_channel_with_no_releases_is_named_as_such():
    reports = check_drift(
        "0.18.1",
        ENVIRONMENTS[:1],
        fetcher_returning({"https://playground-api.example.invalid": None}),
    )

    assert not reports[0].is_behind
    assert "no release published" in format_report(reports[0])


def test_one_unreachable_environment_does_not_hide_the_others():
    def _fetch(api_url: str) -> str | None:
        if "playground" in api_url:
            raise OSError("timed out")
        return "0.11.1"

    reports = check_drift("0.18.1", ENVIRONMENTS, _fetch)

    assert reports[0].is_unreadable
    assert reports[1].is_behind
    assert reports[1].published_version == "0.11.1"


def test_report_names_both_versions_so_the_gap_is_readable():
    reports = check_drift(
        "0.18.1",
        ENVIRONMENTS[:1],
        fetcher_returning({"https://playground-api.example.invalid": "0.16.1"}),
    )

    line = format_report(reports[0])
    assert "0.16.1" in line
    assert "0.18.1" in line
