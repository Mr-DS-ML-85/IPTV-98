import asyncio
import re
from functools import partial
from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "ROXIE"

CACHE_FILE = Cache(f"{TAG.lower()}.json", exp=10_800)

HTML_CACHE = Cache(f"{TAG.lower()}-html.json", exp=19_800)

BASE_URL = "https://roxiestreams.live"

SPORT_ENDPOINTS = {
    "fighting": "Fighting",
    # "mlb": "MLB",
    "motorsports": "Racing",
    "nba": "NBA",
    "nfl": "American Football",
    "nhl": "NHL",
    "soccer": "Soccer",
}


async def process_event(url: str, url_num: int) -> str | None:
    if not (html_data := await network.request(url, log=log)):
        return

    valid_m3u8 = re.compile(
        r"showPlayer\(['\"]clappr['\"],\s*['\"]([^'\"]+?\.m3u8(?:\?[^'\"]*)?)['\"]\)",
        re.IGNORECASE,
    )

    if not (match := valid_m3u8.search(html_data.text)):
        log.info(f"URL {url_num}) No M3U8 found")

        return

    log.info(f"URL {url_num}) Captured M3U8")

    return match[1]


async def refresh_html_cache(
    url: str,
    sport: str,
    now_ts: float,
) -> dict[str, dict[str, str | float]]:

    events = {}

    if not (html_data := await network.request(url, log=log)):
        return events

    soup = HTMLParser(html_data.content)

    for row in soup.css("table#eventsTable tbody tr"):
        if not (a_tag := row.css_first("td a")):
            continue

        event = a_tag.text(strip=True)

        if not (href := a_tag.attributes.get("href")):
            continue

        if not (span := row.css_first("span.countdown-timer")):
            continue

        data_start = span.attributes["data-start"].rsplit(":", 1)[0]

        event_dt = Time.from_str(data_start, timezone="PST")

        event_sport = SPORT_ENDPOINTS[sport]

        key = f"[{event_sport}] {event} ({TAG})"

        events[key] = {
            "sport": event_sport,
            "event": event,
            "link": href,
            "event_ts": event_dt.timestamp(),
            "timestamp": now_ts,
        }

    return events


async def get_events(cached_keys: list[str]) -> list[dict[str, str]]:
    now = Time.clean(Time.now())

    if not (events := HTML_CACHE.load()):
        log.info("Refreshing HTML cache")

        sport_urls = {sport: urljoin(BASE_URL, sport) for sport in SPORT_ENDPOINTS}

        tasks = [
            refresh_html_cache(
                url,
                sport,
                now.timestamp(),
            )
            for sport, url in sport_urls.items()
        ]

        results = await asyncio.gather(*tasks)

        events = {k: v for data in results for k, v in data.items()}

        HTML_CACHE.write(events)

    live = []

    start_ts = now.delta(minutes=-30).timestamp()
    end_ts = now.delta(minutes=30).timestamp()

    for k, v in events.items():
        if k in cached_keys:
            continue

        if not start_ts <= v["event_ts"] <= end_ts:
            continue

        live.append({**v})

    return live


async def scrape() -> None:
    cached_urls = CACHE_FILE.load()

    cached_count = len(cached_urls)

    urls.update(cached_urls)

    log.info(f"Loaded {cached_count} event(s) from cache")

    log.info(f'Scraping from "{BASE_URL}"')

    events = await get_events(cached_urls.keys())

    log.info(f"Processing {len(events)} new URL(s)")

    if events:
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
                sport, event, ts, link = (
                    ev["sport"],
                    ev["event"],
                    ev["event_ts"],
                    ev["link"],
                )

                tvg_id, logo = leagues.get_tvg_info(sport, event)

                key = f"[{sport}] {event} ({TAG})"

                entry = {
                    "url": url,
                    "logo": logo,
                    "base": BASE_URL,
                    "timestamp": ts,
                    "id": tvg_id or "Live.Event.us",
                    "link": link,
                }

                urls[key] = cached_urls[key] = entry

    if new_count := len(cached_urls) - cached_count:
        log.info(f"Collected and cached {new_count} new event(s)")

    else:
        log.info("No new events found")

    CACHE_FILE.write(cached_urls)
