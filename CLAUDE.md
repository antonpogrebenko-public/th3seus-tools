# th3seus-tools

Development utilities for TH3SEUS project.

## Overview

Miscellaneous scripts and tools for development and testing.

## Usage

```bash
# Activate virtual environment
source .venv/bin/activate

# Run main script
python main.py
```

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Files

- `main.py` — Main utility script
- `requirements.txt` — Python dependencies

## Approach
- Read existing files before writing. Don't re-read unless changed.
- Thorough in reasoning, concise in output.
- No sycophantic openers or closing fluff.
- No emojis or em-dashes.
- Do not guess APIs, versions, flags, commit SHAs, or package names. Verify by reading code or docs before asserting.
