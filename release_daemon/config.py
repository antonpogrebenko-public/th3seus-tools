import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ReleaseConfig:
    api_url: str
    release_token: str
    s3_bucket: str
    daemon_dir: str
    artifacts_dir: str | None

    @classmethod
    def from_env(cls, artifacts_dir: str | None = None) -> "ReleaseConfig":
        return cls(
            api_url=os.environ.get("RELEASE_API_URL", "https://api.th3seus.net"),
            release_token=os.environ["HITL_RELEASE_TOKEN"],
            s3_bucket=os.environ.get("RELEASE_S3_BUCKET", "th3seus-artifacts"),
            daemon_dir=os.environ.get("HITL_DAEMON_DIR", os.path.join(os.getcwd(), "..", "hitl-daemon")),
            artifacts_dir=artifacts_dir,
        )
