import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ReleaseConfig:
    api_url: str
    release_token: str
    s3_bucket: str
    daemon_dir: str
    artifacts_dir: str | None

    @staticmethod
    def api_url_from_env() -> str:
        """The release channel to talk to.

        Separate from the full config because reading the channel needs no
        credentials, and a read-only command should not demand a publishing
        token to run.
        """
        return os.environ.get("RELEASE_API_URL", "https://api.th3seus.net")

    @classmethod
    def from_env(cls, artifacts_dir: str | None = None) -> "ReleaseConfig":
        # Empty is rejected as well as missing. A token substituted as an empty
        # string — an `aws ssm get-parameter` that failed inside a shell
        # expansion, say — otherwise sails past this and surfaces as a 403 from
        # the API, which points at the wrong thing entirely.
        token = os.environ.get("HITL_RELEASE_TOKEN", "").strip()
        if not token:
            raise ValueError(
                "HITL_RELEASE_TOKEN is not set, or is set to an empty value. "
                "Publishing needs it; reading the channel does not."
            )
        return cls(
            api_url=cls.api_url_from_env(),
            release_token=token,
            s3_bucket=os.environ.get("RELEASE_S3_BUCKET", "th3seus-artifacts"),
            daemon_dir=os.environ.get(
                "HITL_DAEMON_DIR", os.path.join(os.getcwd(), "..", "hitl-daemon")
            ),
            artifacts_dir=artifacts_dir,
        )
