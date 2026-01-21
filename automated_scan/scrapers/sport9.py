import asyncio
from functools import partial
from urllib.parse import urljoin

from playwright.async_api import async_playwright
from selectolax.parser import HTMLParser

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "SPORT9"

CACHE_FILE = Cache(f"{TAG.lower()}.json", exp=5_400)

BASE_URL = "https://sport9.ru/"


async def get_events(cached_keys: list[str]) -> list[dict[str, str]]:
    now = Time.now()

    tasks = [
        network.request(
            BASE_URL,
            log=log,
            params={"date": d.date()},
        )
        for d in [
            now.delta(days=-1),
            now,
            now.delta(days=1),
        ]
    ]

    results = await asyncio.gather(*tasks)

    events = []

    if not (soups := [HTMLParser(html.content) for html in results if html]):
        return events

    for soup in soups:
        for card in soup.css("a.match-card"):
            live_badge = card.css_first(".live-badge")

            if not live_badge or live_badge.text(strip=True) != "Live":
                continue

            if not (sport_node := card.css_first(".tournament-name")):
                continue

            sport = sport_node.text(strip=True)

            team_1_node = card.css_first(".team1 .team-name")

            team_2_node = card.css_first(".team2 .team-name")

            if team_1_node and team_2_node:
                event = event = (
                    f"{team_1_node.text(strip=True)} vs {team_2_node.text(strip=True)}"
                )

            elif team_1_node:
                event = team_1_node.text(strip=True)

            elif team_2_node:
                event = team_2_node.text(strip=True)

            else:
                continue

            if f"[{sport}] {event} ({TAG})" in cached_keys:
                continue

            if not (href := card.attributes.get("href")):
                continue

            events.append(
                {
                    "sport": sport,
                    "event": event,
                    "link": urljoin(BASE_URL, href),
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
                            "base": "https://vividmosaica.com/",
                            "timestamp": now,
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
