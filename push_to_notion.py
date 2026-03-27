#!/usr/bin/env python3
"""
Write a minimal podcast episode list to a single Notion page as JSON code blocks.
Claude can fetch the whole page in one call and get all episode data.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_PAGE_ID = os.environ["NOTION_PAGE_ID"]
JSON_PATH = Path("rss_output/all_recent_items.json")
CHUNK_SIZE = 1900  # Notion block text limit is 2000 chars

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}


def get_block_children(block_id: str) -> list[dict]:
    url = f"https://api.notion.com/v1/blocks/{block_id}/children"
    children = []
    start_cursor = None
    while True:
        params = {"page_size": 100}
        if start_cursor:
            params["start_cursor"] = start_cursor
        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        children.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        start_cursor = data.get("next_cursor")
    return children


def delete_block(block_id: str) -> None:
    requests.delete(
        f"https://api.notion.com/v1/blocks/{block_id}",
        headers=HEADERS,
        timeout=30,
    ).raise_for_status()


def append_code_blocks(page_id: str, text: str) -> None:
    """Append the full text split into <=1900-char code blocks."""
    chunks = [text[i:i + CHUNK_SIZE] for i in range(0, len(text), CHUNK_SIZE)]
    children = [
        {
            "object": "block",
            "type": "code",
            "code": {
                "rich_text": [{"type": "text", "text": {"content": chunk}}],
                "language": "json",
            },
        }
        for chunk in chunks
    ]
    # Notion allows max 100 blocks per append call
    for i in range(0, len(children), 100):
        resp = requests.patch(
            f"https://api.notion.com/v1/blocks/{page_id}/children",
            headers=HEADERS,
            json={"children": children[i:i + 100]},
            timeout=30,
        )
        resp.raise_for_status()


def main() -> None:
    if not JSON_PATH.exists():
        print(f"ERROR: {JSON_PATH} not found.", file=sys.stderr)
        sys.exit(1)

    with JSON_PATH.open("r", encoding="utf-8") as f:
        episodes: list[dict] = json.load(f)

    print(f"Loaded {len(episodes)} episodes.")

    payload = json.dumps(episodes, ensure_ascii=False, indent=2)
    print(f"Payload size: {len(payload)} chars → {len(payload) // CHUNK_SIZE + 1} block(s)")

    # Clear existing blocks on the page
    existing = get_block_children(NOTION_PAGE_ID)
    print(f"Deleting {len(existing)} existing block(s)...")
    for block in existing:
        delete_block(block["id"])

    # Write fresh content
    append_code_blocks(NOTION_PAGE_ID, payload)
    print("Done. Notion page updated.")


if __name__ == "__main__":
    main()
