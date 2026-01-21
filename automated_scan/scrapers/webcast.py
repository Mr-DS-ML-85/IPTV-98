import asyncio
from functools import partial

from playwright.async_api import async_playwright
from selectolax.parser import HTMLParser

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "WEBCAST"

CACHE_FILE = Cache(f"{TAG.lower()}.json", exp=10_800)

HTML_CACHE = Cache(f"{TAG.lower()}-html.json", exp=86_400)

BASE_URLS = {"NFL": "https://nflwebcast.com", "NHL": "https://slapstreams.com"}


def fix_event(s: str) -> str:
    return " vs ".join(s.split("@"))


async def refresh_html_cache(url: str) -> dict[str, dict[str, str | float]]:
    events = {}

    if not (html_data := await network.request(url, log=log)):
        return events

    now = Time.clean(Time.now())

    soup = HTMLParser(html_data.content)

    title = soup.css_first("title").text(strip=True)

    sport = "NFL" if "NFL" in title else "NHL"

    date_text = now.strftime("%B %d, %Y")

    if date_row := soup.css_first("tr.mdatetitle"):
        if mtdate_span := date_row.css_first("span.mtdate"):
            date_text = mtdate_span.text(strip=True)

    for row in soup.css("tr.singele_match_date"):
        if not (time_node := row.css_first("td.matchtime")):
            continue

        time = time_node.text(strip=True)

        if not (vs_node := row.css_first("td.teamvs a")):
            continue

        event_name = vs_node.text(strip=True)

        for span in vs_node.css("span.mtdate"):
            date = span.text(strip=True)

            event_name = event_name.replace(date, "").strip()

        if not (href := vs_node.attributes.get("href")):
            continue

        event_dt = Time.from_str(f"{date_text} {time} PM", timezone="EST")

        event = fix_event(event_name)

        key = f"[{sport}] {event} ({TAG})"

        events[key] = {
            "sport": sport,
            "event": event,
            "link": href,
            "event_ts": event_dt.timestamp(),
            "timestamp": now.timestamp(),
        }

    return events


async def get_events(cached_keys: list[str]) -> list[dict[str, str]]:
    now = Time.clean(Time.now())

    if not (events := HTML_CACHE.load()):
        log.info("Refreshing HTML cache")

        tasks = [refresh_html_cache(url) for url in BASE_URLS.values()]

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

    log.info(f'Scraping from "{' & '.join(BASE_URLS.values())}"')

    events = await get_events(cached_urls.keys())

    log.info(f"Processing {len(events)} new URL(s)")

    if events:
        async with async_playwright() as p:
            browser, context = await network.browser(p)

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
                            ev["event_ts"],
                            ev["link"],
                        )

                        key = f"[{sport}] {event} ({TAG})"

                        tvg_id, logo = leagues.get_tvg_info(sport, event)

                        entry = {
                            "url": url,
                            "logo": logo,
                            "base": BASE_URLS[sport],
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
