import base64
import re
from functools import partial
from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "STRMBTW"

CACHE_FILE = Cache(f"{TAG.lower()}.json", exp=3_600)

BASE_URLS = ["https://hiteasport.info/", "https://streambtw.com/"]


def fix_league(s: str) -> str:
    pattern = re.compile(r"^\w*-\w*", re.IGNORECASE)

    return " ".join(s.split("-")) if pattern.search(s) else s


async def process_event(url: str, url_num: int) -> str | None:
    if not (html_data := await network.request(url, log=log)):
        return

    valid_m3u8 = re.compile(r'var\s+(\w+)\s*=\s*"([^"]*)"', re.IGNORECASE)

    if not (match := valid_m3u8.search(html_data.text)):
        log.info(f"URL {url_num}) No M3U8 found")

        return

    stream_link: str = match[2]

    if not stream_link.startswith("http"):
        stream_link = base64.b64decode(stream_link).decode("utf-8")

    log.info(f"URL {url_num}) Captured M3U8")

    return stream_link


async def get_events(url: str) -> list[dict[str, str]]:
    events = []

    if not (html_data := await network.request(url, log=log)):
        return events

    soup = HTMLParser(html_data.content)

    for card in soup.css(".league"):
        if not (league_elem := card.css_first(".league-header h4")):
            continue

        for event in card.css(".match"):
            if not (event_elem := event.css_first(".match-title")):
                continue

            if not (watch_btn := event.css_first(".watch-btn")) or not (
                href := watch_btn.attributes.get("href")
            ):
                continue

            league, name = league_elem.text(strip=True), event_elem.text(strip=True)

            events.append(
                {
                    "sport": fix_league(league),
                    "event": name,
                    "link": urljoin(url, href),
                }
            )

    return events


async def scrape() -> None:
    if cached := CACHE_FILE.load():
        urls.update(cached)

        log.info(f"Loaded {len(urls)} event(s) from cache")

        return

    if not (base_url := await network.get_base(BASE_URLS)):
        log.warning("No working StreamBTW mirrors")

        CACHE_FILE.write(urls)

        return

    log.info(f'Scraping from "{base_url}"')

    events = await get_events(base_url)

    log.info(f"Processing {len(events)} new URL(s)")

    if events:
        now = Time.clean(Time.now())

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
                    "timestamp": now.timestamp(),
                    "id": tvg_id or "Live.Event.us",
                    "link": link,
                }

                urls[key] = entry

    log.info(f"Collected {len(urls)} event(s)")

    CACHE_FILE.write(urls)
