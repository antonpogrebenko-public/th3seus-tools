import re
import subprocess

import yaml


def load_filter_config(filter_path: str) -> dict:
    try:
        with open(filter_path) as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {"exclude_paths": [], "exclude_keywords": []}


def get_commits_since_tag(tag: str, repo_dir: str) -> list[dict]:
    result = subprocess.run(
        ["git", "log", f"{tag}..HEAD", "--pretty=format:%H|%s", "--no-merges"],
        capture_output=True,
        text=True,
        cwd=repo_dir,
    )
    if result.returncode != 0:
        return []

    commits = []
    for line in result.stdout.strip().splitlines():
        if "|" not in line:
            continue
        sha, message = line.split("|", 1)
        commits.append({"sha": sha[:7], "message": message})
    return commits


def get_changed_files(sha: str, repo_dir: str) -> list[str]:
    result = subprocess.run(
        ["git", "diff-tree", "--no-commit-id", "--name-only", "-r", sha],
        capture_output=True,
        text=True,
        cwd=repo_dir,
    )
    return result.stdout.strip().splitlines() if result.returncode == 0 else []


def filter_commits(commits: list[dict], filter_config: dict, repo_dir: str) -> list[dict]:
    exclude_paths = filter_config.get("exclude_paths", [])
    exclude_keywords = [kw.lower() for kw in filter_config.get("exclude_keywords", [])]

    filtered = []
    for commit in commits:
        msg_lower = commit["message"].lower()
        if any(kw in msg_lower for kw in exclude_keywords):
            continue

        files = get_changed_files(commit["sha"], repo_dir)
        if files and all(
            any(re.match(pattern.replace("*", ".*"), f) for pattern in exclude_paths)
            for f in files
        ):
            continue

        filtered.append(commit)
    return filtered


def format_commit_summary(commits: list[dict]) -> str:
    if not commits:
        return ""
    return "\n".join(f"- {c['message']} ({c['sha']})" for c in commits)


def get_previous_tag(repo_dir: str) -> str | None:
    result = subprocess.run(
        ["git", "describe", "--tags", "--abbrev=0", "HEAD^"],
        capture_output=True,
        text=True,
        cwd=repo_dir,
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip()
