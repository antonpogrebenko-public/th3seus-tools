"""Ad-hoc code signing for release artifacts.

Paid certificates are deliberately deferred (see the release runbook), so this
module does not authenticate a publisher. It exists for one narrower reason:
Apple Silicon refuses to execute an arm64 binary carrying no signature at all,
and `codesign -s -` satisfies that for free.

Linux ELF and Windows PE have no equivalent free mechanism. Artifacts for those
platforms are published unsigned, and nothing here may report them as signed.
"""

import subprocess
from pathlib import Path

# Platforms whose loader rejects a wholly unsigned binary. Only macOS qualifies:
# requiring a signature elsewhere would be unmeetable without buying a
# certificate, and a check that cannot pass is worse than no check.
_REQUIRES_SIGNATURE = {"macos"}


class SigningError(RuntimeError):
    """An artifact is missing a signature its target platform requires."""


def platform_requires_signature(platform: str) -> bool:
    return platform in _REQUIRES_SIGNATURE


def adhoc_sign(platform: str, path: Path) -> None:
    """Apply an ad-hoc signature in place. No-op where the platform has none.

    `--force` replaces any existing signature, which keeps re-publishing the
    same artifact idempotent.
    """
    if not platform_requires_signature(platform):
        return

    result = subprocess.run(
        ["codesign", "--sign", "-", "--force", str(path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SigningError(
            f"Ad-hoc signing failed for {path.name}: {result.stderr.strip()}"
        )


def verify_signed(platform: str, path: Path) -> None:
    """Raise SigningError when a platform that requires a signature lacks one.

    Passes silently for platforms with no free signing mechanism — unsigned is
    the correct state there, not a failure.
    """
    if not platform_requires_signature(platform):
        return

    result = subprocess.run(
        ["codesign", "--display", "--verbose=2", str(path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SigningError(
            f"{path.name} carries no code signature; Apple Silicon will refuse "
            f"to execute it. codesign said: {result.stderr.strip()}"
        )
