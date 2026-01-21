from functools import partial

from playwright.async_api import async_playwright

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "EMBEDHD"

CACHE_FILE = Cache(f"{TAG.lower()}.json", exp=5_400)

API_CACHE = Cache(f"{TAG.lower()}-api.json", exp=28_800)

BASE_URL = "https://embedhd.org/api-event.php"


def fix_league(s: str) -> str:
    return " ".join(x.capitalize() for x in s.split()) if len(s) > 5 else s.upper()


async def get_events(cached_keys: list[str]) -> list[dict[str, str]]:
    now = Time.clean(Time.now())

    if not (api_data := API_CACHE.load(per_entry=False)):
        log.info("Refreshing API cache")

        api_data = {"timestamp": now.timestamp()}

        if r := await network.request(BASE_URL, log=log):
            api_data: dict = r.json()

            api_data["timestamp"] = now.timestamp()

        API_CACHE.write(api_data)

    events = []

    start_dt = now.delta(hours=-3)
    end_dt = now.delta(minutes=30)

    for info in api_data.get("days", []):
        for event in info["items"]:
            if (event_league := event["league"]) == "channel tv":
                continue

            event_dt = Time.from_str(event["when_et"], timezone="ET")

            if not start_dt <= event_dt <= end_dt:
                continue

            sport = fix_league(event_league)

            event_name = event["title"]

            if f"[{sport}] {event_name} ({TAG})" in cached_keys:
                continue

            event_streams: list[dict[str, str]] = event["streams"]

            if not (event_link := event_streams[0].get("link")):
                continue

            events.append(
                {
                    "sport": sport,
                    "event": event_name,
                    "link": event_link,
                    "timestamp": now.timestamp(),
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
                        sport, event, link, ts = (
                            ev["sport"],
                            ev["event"],
                            ev["link"],
                            ev["timestamp"],
                        )

                        tvg_id, logo = leagues.get_tvg_info(sport, event)

                        key = f"[{sport}] {event} ({TAG})"

                        entry = {
                            "url": url,
                            "logo": logo,
                            "base": "https://vividmosaica.com/",
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
