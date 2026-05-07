import re


def parse_changelog(content: str, version: str) -> str | None:
    pattern = rf"## \[{re.escape(version)}\].*?\n(.*?)(?=\n## \[|\Z)"
    match = re.search(pattern, content, re.DOTALL)
    if not match:
        return None
    return match.group(1).strip()


def read_version_from_cargo_toml(cargo_toml_content: str) -> str:
    match = re.search(r'^version\s*=\s*"([^"]+)"', cargo_toml_content, re.MULTILINE)
    if not match:
        raise ValueError("Could not find version in Cargo.toml")
    return match.group(1)
