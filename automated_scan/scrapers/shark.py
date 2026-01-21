import re
from functools import partial

from selectolax.parser import HTMLParser

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "SHARK"

CACHE_FILE = Cache(f"{TAG.lower()}.json", exp=10_800)

HTML_CACHE = Cache(f"{TAG.lower()}-html.json", exp=19_800)

BASE_URL = "https://sharkstreams.net"


async def process_event(url: str, url_num: int) -> str | None:
    if not (r := await network.request(url, log=log)):
        log.info(f"URL {url_num}) Failed to load url.")

        return

    data: dict[str, list[str]] = r.json()

    if not (urls := data.get("urls")):
        log.info(f"URL {url_num}) No M3U8 found")

        return

    log.info(f"URL {url_num}) Captured M3U8")

    return urls[0]


async def refresh_html_cache(now_ts: float) -> dict[str, dict[str, str | float]]:
    log.info("Refreshing HTML cache")

    events = {}

    if not (html_data := await network.request(BASE_URL, log=log)):
        return events

    pattern = re.compile(r"openEmbed\('([^']+)'\)", re.IGNORECASE)

    soup = HTMLParser(html_data.content)

    for row in soup.css(".row"):
        date_node = row.css_first(".ch-date")

        sport_node = row.css_first(".ch-category")
        name_node = row.css_first(".ch-name")

        if not (date_node and sport_node and name_node):
            continue

        event_dt = Time.from_str(date_node.text(strip=True), timezone="EST")

        sport = sport_node.text(strip=True)

        event_name = name_node.text(strip=True)

        embed_btn = row.css_first("a.hd-link.secondary")

        if not embed_btn or not (onclick := embed_btn.attributes.get("onclick")):
            continue

        if not (match := pattern.search(onclick)):
            continue

        link = match[1].replace("player.php", "get-stream.php")

        key = f"[{sport}] {event_name} ({TAG})"

        events[key] = {
            "sport": sport,
            "event": event_name,
            "link": link,
            "event_ts": event_dt.timestamp(),
            "timestamp": now_ts,
        }

    return events


async def get_events(cached_keys: list[str]) -> list[dict[str, str]]:
    now = Time.clean(Time.now())

    if not (events := HTML_CACHE.load()):
        events = await refresh_html_cache(now.timestamp())

        HTML_CACHE.write(events)

    live = []

    start_ts = now.delta(hours=-1).timestamp()
    end_ts = now.delta(minutes=10).timestamp()

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
