import base64
import re
from functools import partial

import feedparser
from selectolax.parser import HTMLParser

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "PAWA"

CACHE_FILE = Cache(f"{TAG.lower()}.json", exp=10_800)

BASE_URL = "https://pawastreams.net/feed"


async def process_event(url: str, url_num: int) -> str | None:
    if not (event_data := await network.request(url, log=log)):
        log.info(f"URL {url_num}) Failed to load url.")

        return

    soup = HTMLParser(event_data.content)

    if not (iframe := soup.css_first("iframe")):
        log.warning(f"URL {url_num}) No iframe element found.")

        return

    if not (iframe_src := iframe.attributes.get("src")):
        log.warning(f"URL {url_num}) No iframe source found.")

        return

    if not (iframe_src_data := await network.request(iframe_src, log=log)):
        log.info(f"URL {url_num}) Failed to load iframe source.")

        return

    pattern = re.compile(r"source:\s*window\.atob\(\s*'([^']+)'\s*\)", re.IGNORECASE)

    if not (match := pattern.search(iframe_src_data.text)):
        log.warning(f"URL {url_num}) No Clappr source found.")

        return

    log.info(f"URL {url_num}) Captured M3U8")

    return base64.b64decode(match[1]).decode("utf-8")


async def get_events(cached_keys: list[str]) -> list[dict[str, str]]:
    events = []

    if not (html_data := await network.request(BASE_URL, log=log)):
        return events

    feed = feedparser.parse(html_data.content)

    for entry in feed.entries:
        if not (link := entry.get("link")):
            continue

        if not (title := entry.get("title")):
            continue

        sport = "Live Event"

        title = title.replace(" v ", " vs ")

        if f"[{sport}] {title} ({TAG})" in cached_keys:
            continue

        events.append(
            {
                "sport": sport,
                "event": title,
                "link": link,
            }
        )

    return events


async def scrape() -> None:
    cached_urls = CACHE_FILE.load()

    cached_count = len(cached_urls)

    urls.update(cached_urls)

    log.info(f"Loaded {cached_count} event(s) from cache")

    log.info(f'Scraping from "{BASE_URL}"')

    events = await get_events(cached_urls.keys())

    log.info(f"Processing {len(events)} new URL(s)")

    if events:
        now = Time.clean(Time.now()).timestamp()

        for i, ev in enumerate(events, start=1):
            handler = partial(
                process_event,
                url=ev["link"],
                url_num=i,
            )

            url = await network.safe_process(
                handler,
                url_num=i,
                semaphore=network.HTTP_S,
                log=log,
            )

            if url:
                sport, event, link = (
                    ev["sport"],
                    ev["event"],
                    ev["link"],
                )

                key = f"[{sport}] {event} ({TAG})"

                tvg_id, logo = leagues.get_tvg_info(sport, event)

                entry = {
                    "url": url,
                    "logo": logo,
                    "base": link,
                    "timestamp": now,
                    "id": tvg_id or "Live.Event.us",
                    "link": link,
                }

                urls[key] = cached_urls[key] = entry

    if new_count := len(cached_urls) - cached_count:
        log.info(f"Collected and cached {new_count} new event(s)")

    else:
        log.info("No new events found")

    CACHE_FILE.write(cached_urls)
