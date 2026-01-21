import asyncio
from functools import partial
from urllib.parse import urljoin

from playwright.async_api import async_playwright
from selectolax.parser import HTMLParser

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "STRMHUB"

CACHE_FILE = Cache(f"{TAG.lower()}.json", exp=10_800)

HTML_CACHE = Cache(f"{TAG.lower()}-html.json", exp=28_800)

BASE_URL = "https://streamhub.pro/"

CATEGORIES = {
    "Soccer": "sport_68c02a4464a38",
    "American Football": "sport_68c02a4465113",
    # "Baseball": "sport_68c02a446582f",
    "Basketball": "sport_68c02a4466011",
    "Cricket": "sport_68c02a44669f3",
    "Hockey": "sport_68c02a4466f56",
    "MMA": "sport_68c02a44674e9",
    "Racing": "sport_68c02a4467a48",
    # "Rugby": "sport_68c02a4467fc1",
    "Tennis": "sport_68c02a4468cf7",
    # "Volleyball": "sport_68c02a4469422",
}


async def refresh_html_cache(
    date: str,
    sport_id: str,
    ts: float,
) -> dict[str, dict[str, str | float]]:
    events = {}

    if not (
        html_data := await network.request(
            urljoin(BASE_URL, f"events/{date}"),
            log=log,
            params={"sport_id": sport_id},
        )
    ):
        return events

    soup = HTMLParser(html_data.content)

    for section in soup.css(".events-section"):
        if not (sport_node := section.css_first(".section-titlte")):
            continue

        sport = sport_node.text(strip=True)

        logo = section.css_first(".league-icon img").attributes.get("src")

        for event in section.css(".section-event"):
            event_name = "Live Event"

            if teams := event.css_first(".event-competitors"):
                home, away = teams.text(strip=True).split("vs.")

                event_name = f"{away} vs {home}"

            if not (event_button := event.css_first(".event-button a")) or not (
                href := event_button.attributes.get("href")
            ):
                continue

            event_date = event.css_first(".event-countdown").attributes.get(
                "data-start"
            )

            event_dt = Time.from_str(event_date, timezone="UTC")

            key = f"[{sport}] {event_name} ({TAG})"

            events[key] = {
                "sport": sport,
                "event": event_name,
                "link": href,
                "logo": logo,
                "event_ts": event_dt.timestamp(),
                "timestamp": ts,
            }

    return events


async def get_events(cached_keys: list[str]) -> list[dict[str, str]]:
    now = Time.clean(Time.now())

    if not (events := HTML_CACHE.load()):
        log.info("Refreshing HTML cache")

        tasks = [
            refresh_html_cache(
                date,
                sport_id,
                now.timestamp(),
            )
            for date in [now.date(), now.delta(days=1).date()]
            for sport_id in CATEGORIES.values()
        ]

        results = await asyncio.gather(*tasks)

        events = {k: v for data in results for k, v in data.items()}

        HTML_CACHE.write(events)

    live = []

    start_ts = now.delta(hours=-1).timestamp()
    end_ts = now.delta(minutes=1).timestamp()

    for k, v in events.items():
        if k in cached_keys:
            continue

        if not start_ts <= v["event_ts"] <= end_ts:
            continue

        live.append({**v})

    return live


async def scrape() -> None:
    cached_urls = CACHE_FILE.load()

    valid_urls = {k: v for k, v in cached_urls.items() if v["url"]}

    valid_count = cached_count = len(valid_urls)

    urls.update(valid_urls)

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
                        timeout=5,
                        log=log,
                    )

                    url = await network.safe_process(
                        handler,
                        url_num=i,
                        semaphore=network.PW_S,
                        log=log,
                    )

                    sport, event, logo, link, ts = (
                        ev["sport"],
                        ev["event"],
                        ev["logo"],
                        ev["link"],
                        ev["event_ts"],
                    )

                    key = f"[{sport}] {event} ({TAG})"

                    tvg_id, pic = leagues.get_tvg_info(sport, event)

                    entry = {
                        "url": url,
                        "logo": logo or pic,
                        "base": "https://storytrench.net/",
                        "timestamp": ts,
                        "id": tvg_id or "Live.Event.us",
                        "link": link,
                    }

                    cached_urls[key] = entry

                    if url:
                        valid_count += 1

                        urls[key] = entry

            finally:
                await browser.close()

    if new_count := valid_count - cached_count:
        log.info(f"Collected and cached {new_count} new event(s)")

    else:
        log.info("No new events found")

    CACHE_FILE.write(cached_urls)
