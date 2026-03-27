#!/usr/bin/env python3

from __future__ import annotations

import argparse
from email import parser
import json
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

import requests


DEFAULT_FEEDS = [
    "https://feeds.transistor.fm/acquired",
]

USER_AGENT = "RSSWeeklyScanner/1.0 (+personal use)"
TIMEOUT_SECONDS = 20


def safe_name_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.replace(":", "_")
    path = parsed.path.strip("/") or "root"
    combined = f"{host}_{path}"
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", combined)[:180]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_feeds(config_path: Path | None) -> list[str]:
    if config_path is None:
        return DEFAULT_FEEDS

    with config_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    feeds = data.get("feeds", [])
    if not isinstance(feeds, list) or not all(isinstance(x, str) for x in feeds):
        raise ValueError("Config must contain a 'feeds' list of strings.")
    return feeds


def fetch_feed(url: str, session: requests.Session) -> bytes:
    response = session.get(url, timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.content


def save_raw_xml(content: bytes, feed_url: str, output_dir: Path) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    feed_name = safe_name_from_url(feed_url)
    file_path = output_dir / f"{feed_name}__{timestamp}.xml"
    file_path.write_bytes(content)
    return file_path


def strip_tag(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def get_child_text(elem: ET.Element, tag_name: str) -> str | None:
    for child in elem:
        if strip_tag(child.tag) == tag_name:
            return (child.text or "").strip()
    return None


def find_itunes_duration(elem: ET.Element) -> str | None:
    for child in elem:
        if strip_tag(child.tag) == "duration":
            return (child.text or "").strip()
    return None


def find_enclosure_url(elem: ET.Element) -> str | None:
    for child in elem:
        if strip_tag(child.tag) == "enclosure":
            return child.attrib.get("url")
    return None


def parse_pub_date(date_str: str | None) -> datetime | None:
    if not date_str:
        return None

    # Try RSS-style dates first
    try:
        dt = parsedate_to_datetime(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # Try ISO format as fallback
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def parse_feed_items(xml_content: bytes, feed_url: str) -> tuple[str | None, list[dict]]:
    root = ET.fromstring(xml_content)

    channel = None
    for elem in root.iter():
        if strip_tag(elem.tag) == "channel":
            channel = elem
            break

    podcast_title = get_child_text(channel, "title") if channel is not None else None
    items: list[dict] = []

    for elem in root.iter():
        if strip_tag(elem.tag) != "item":
            continue

        title = get_child_text(elem, "title")
        link = get_child_text(elem, "link")
        guid = get_child_text(elem, "guid")
        description = get_child_text(elem, "description")
        pub_date_raw = get_child_text(elem, "pubDate")
        author = get_child_text(elem, "author")
        duration = find_itunes_duration(elem)
        enclosure_url = find_enclosure_url(elem)
        pub_date = parse_pub_date(pub_date_raw)

        items.append(
            {
                "podcast_title": podcast_title,
                "feed_url": feed_url,
                "title": title,
                "link": link,
                "guid": guid,
                "description": description,
                "author": author,
                "duration": duration,
                "enclosure_url": enclosure_url,
                "pub_date_raw": pub_date_raw,
                "pub_date_utc": pub_date.isoformat() if pub_date else None,
            }
        )

    return podcast_title, items


def filter_recent_items(items: list[dict], days: int) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    filtered = []

    for item in items:
        raw = item.get("pub_date_utc")
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(raw)
        except Exception:
            continue
        if dt >= cutoff:
            filtered.append(item)

    return filtered


def save_json(data: object, path: Path) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()

    project_dir = Path(__file__).resolve().parent
    

    parser.add_argument(
    "--config",
    type=Path,
    default=project_dir / "feeds.json",
    help="Path to feeds.json",
    )
    
    parser.add_argument("--output", type=Path, default=project_dir / "rss_output")
    parser.add_argument("--days", type=int, default=7, help="Keep only entries from last N days")
    args = parser.parse_args()

    try:
        feeds = load_feeds(args.config)
    except Exception as e:
        print(f"Failed to load config: {e}", file=sys.stderr)
        return 1

    if not feeds:
        print("No feeds configured.", file=sys.stderr)
        return 1

    raw_dir = args.output / "raw_xml"
    parsed_dir = args.output / "parsed"
    ensure_dir(raw_dir)
    ensure_dir(parsed_dir)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    all_recent_items: list[dict] = []
    manifest: list[dict] = []

    for url in feeds:
        print(f"Fetching {url}")
        try:
            xml_content = fetch_feed(url, session)
            raw_xml_path = save_raw_xml(xml_content, url, raw_dir)

            podcast_title, items = parse_feed_items(xml_content, url)
            recent_items = filter_recent_items(items, args.days)

            feed_name = safe_name_from_url(url)
            recent_json_path = parsed_dir / f"{feed_name}__last_{args.days}_days.json"
            save_json(recent_items, recent_json_path)

            manifest.append(
                {
                    "feed_url": url,
                    "podcast_title": podcast_title,
                    "raw_xml_path": str(raw_xml_path),
                    "recent_json_path": str(recent_json_path),
                    "total_items_in_feed": len(items),
                    "recent_items_kept": len(recent_items),
                    "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                }
            )

            all_recent_items.extend(recent_items)
            print(f"  total items: {len(items)} | kept recent: {len(recent_items)}")

        except Exception as e:
            manifest.append(
                {
                    "feed_url": url,
                    "error": str(e),
                    "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                }
            )
            print(f"  failed: {e}")

        time.sleep(1)

    save_json(all_recent_items, args.output / "all_recent_items.json")

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")

    archive_dir = args.output / "archive"
    ensure_dir(archive_dir)

    save_json(all_recent_items, archive_dir / f"all_recent_items__{timestamp}.json")
    save_json(manifest, archive_dir / f"manifest__{timestamp}.json")

    save_json(manifest, args.output / "manifest.json")


    print(f"\nDone. Output saved in {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())