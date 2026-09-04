"""Ask the web deploy workflow to publish a fresh export.

The site is a static export whose catalogue is read from this API at build
time. Data that changes without a commit therefore leaves the published site
stale, showing last week's components with nothing reporting it: the site
answers 200 throughout, and the only symptom is data that is quietly old.

Anything that changes catalogue data calls `request_web_rebuild` on success,
which fires a `catalogue-changed` repository dispatch at th3seus-web. The
receiving side is `repository_dispatch` in th3seus-web's deploy workflows.

Publishing a daemon release is one of those things: the catalogue reads
`/api/hitl/releases`, so the releases page is prerendered from it. Publishing
0.23.0 to production on 2026-09-04 without a rebuild left that page offering
0.18.1 while the API reported 0.23.0 and the client's minimum supported version
had moved to 0.22.1 -- the site told a visitor their daemon was too old and then
handed them an older one.

The token needs `contents: write` on th3seus-web and nothing else. It is read
from the environment and never stored here.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

WEB_REPO = "antonpogrebenko-public/th3seus-web"
EVENT_TYPE = "catalogue-changed"
TOKEN_VAR = "WEB_REBUILD_TOKEN"


def request_web_rebuild(reason: str, *, repo: str = WEB_REPO) -> bool:
    """Fire the dispatch. Returns whether the site was actually asked to rebuild.

    Never raises, and never exits. The caller has already written data by the
    time this runs, so a failure here does not undo anything and must not be
    reported as though the data change failed. What it must not do is stay
    quiet: an unsent dispatch is exactly the silent staleness this exists to
    prevent, so every path prints what happened and the return value says
    whether a rebuild was requested.
    """
    token = os.environ.get(TOKEN_VAR, "").strip()
    if not token:
        print(
            f"\n[web] {TOKEN_VAR} is not set, so no rebuild was requested.\n"
            f"[web] The data changed but the published site will keep serving its\n"
            f"[web] previous export until something rebuilds it. Set {TOKEN_VAR}, or\n"
            f"[web] run the 'Deploy Staging' workflow in {repo} by hand."
        )
        return False

    request = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/dispatches",
        data=json.dumps({"event_type": EVENT_TYPE, "client_payload": {"reason": reason}}).encode(),
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            # A dispatch is accepted with 204 and no body.
            if response.status == 204:
                print(f"\n[web] Requested a rebuild of {repo} ({reason}).")
                return True
            print(f"\n[web] Dispatch returned HTTP {response.status}; no rebuild was requested.")
            return False
    except urllib.error.HTTPError as e:
        detail = e.read()[:200].decode(errors="replace")
        print(
            f"\n[web] Could not request a rebuild: HTTP {e.code}. {detail}\n"
            f"[web] The data change succeeded. The site will keep serving its previous\n"
            f"[web] export until the workflow is run."
        )
        return False
    except (urllib.error.URLError, OSError) as e:
        print(
            f"\n[web] Could not reach GitHub to request a rebuild: {e}\n"
            f"[web] The data change succeeded; the site is not yet rebuilt."
        )
        return False
