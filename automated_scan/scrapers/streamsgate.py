import asyncio
from functools import partial
from itertools import chain
from typing import Any
from urllib.parse import urljoin

from playwright.async_api import async_playwright

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "STRMSGATE"

CACHE_FILE = Cache(f"{TAG.lower()}.json", exp=10_800)

API_FILE = Cache(f"{TAG.lower()}-api.json", exp=19_800)

BASE_URL = "https://streamingon.org"

SPORT_ENDPOINTS = [
    "soccer",
    "nfl",
    "nba",
    "cfb",
    "mlb",
    "nhl",
    "ufc",
    "boxing",
    "f1",
]


def get_event(t1: str, t2: str) -> str:
    match t1:
        case "RED ZONE":
            return "NFL RedZone"

        case "TBD":
            return "TBD"

        case _:
            return f"{t1.strip()} vs {t2.strip()}"


async def refresh_api_cache(now_ts: float) -> list[dict[str, Any]]:
    log.info("Refreshing API cache")

    tasks = [
        network.request(
            urljoin(BASE_URL, f"data/{sport}.json"),
            log=log,
        )
        for sport in SPORT_ENDPOINTS
    ]

    results = await asyncio.gather(*tasks)

    if not (data := [*chain.from_iterable(r.json() for r in results if r)]):
        return [{"timestamp": now_ts}]

    for ev in data:
        ev["ts"] = ev.pop("timestamp")

        data[-1]["timestamp"] = now_ts

    return data


async def get_events(cached_keys: list[str]) -> list[dict[str, str]]:
    now = Time.clean(Time.now())

    if not (api_data := API_FILE.load(per_entry=False, index=-1)):
        api_data = await refresh_api_cache(now.timestamp())

        API_FILE.write(api_data)

    events = []

    start_dt = now.delta(hours=-1)
    end_dt = now.delta(minutes=5)

    for stream_group in api_data:
        date = stream_group.get("time")

        sport = stream_group.get("league")

        t1, t2 = stream_group.get("away"), stream_group.get("home")

        event = get_event(t1, t2)

        if not (date and sport):
            continue

        if f"[{sport}] {event} ({TAG})" in cached_keys:
            continue

        event_dt = Time.from_str(date, timezone="UTC")

        if not start_dt <= event_dt <= end_dt:
            continue

        if not (streams := stream_group.get("streams")):
            continue

        if not (url := streams[0].get("url")):
            continue

        events.append(
            {
                "sport": sport,
                "event": event,
                "link": url,
                "timestamp": event_dt.timestamp(),
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
        async with async_playwright() as p:
            browser, context = await network.browser(p, browser="external")

            try:
                for i, ev in enumerate(events, start=1):
                    handler = partial(
                        network.process_event,
                        url=ev["link"],
                        url_num=i,
                        context=context,
                        log=log,
                    )

                    url = await network.safe_process(
                        handler,
                        url_num=i,
                        semaphore=network.PW_S,
                        log=log,
                    )

                    if url:
                        sport, event, ts, link = (
                            ev["sport"],
                            ev["event"],
                            ev["timestamp"],
                            ev["link"],
                        )

                        key = f"[{sport}] {event} ({TAG})"

                        tvg_id, logo = leagues.get_tvg_info(sport, event)

                        entry = {
                            "url": url,
                            "logo": logo,
                            "base": BASE_URL,
                            "timestamp": ts,
                            "id": tvg_id or "Live.Event.us",
                            "link": link,
                        }

                        urls[key] = cached_urls[key] = entry

            finally:
                await browser.close()

    if new_count := len(cached_urls) - cached_count:
        log.info(f"Collected and cached {new_count} new event(s)")

    else:
        log.info("No new events found")

    CACHE_FILE.write(cached_urls)
